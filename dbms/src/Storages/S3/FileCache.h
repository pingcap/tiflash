// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Encryption/RandomAccessFile.h>
#include <Interpreters/Settings_fwd.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Server/StorageConfigParser.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/S3/S3Filename.h>
#include <common/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <magic_enum.hpp>
#include <mutex>
#include <unordered_map>

namespace DB
{
class FileSegment
{
public:
    enum class Status
    {
        Empty,
        Complete,
        Failed,
    };

    enum class FileType : UInt64
    {
        Unknow = 0,
        Meta,
        Merged,
        Index,
        Mark, // .mkr, .null.mrk
        NullMap,
        DeleteMarkColData,
        VersionColData,
        HandleColData,
        ColData,
    };

    FileSegment(const String & local_fname_, Status status_, UInt64 size_, FileType file_type_)
        : local_fname(local_fname_)
        , status(status_)
        , size(size_)
        , file_type(file_type_)
        , last_access_time(std::chrono::system_clock::now())
    {}

    bool isReadyToRead() const
    {
        std::lock_guard lock(mtx);
        return status == Status::Complete;
    }

    Status waitForNotEmpty();

    void setSize(UInt64 size_)
    {
        std::lock_guard lock(mtx);
        size = size_;
    }

    void setStatus(Status s)
    {
        std::lock_guard lock(mtx);
        status = s;
        if (status != Status::Empty)
            cv_ready.notify_all();
    }

    UInt64 getSize() const
    {
        std::lock_guard lock(mtx);
        return size;
    }

    const String & getLocalFileName() const
    {
        // `local_filename` is read-only, no need for a lock.
        return local_fname;
    }

    FileType getFileType() const
    {
        // `file_type` is read-only, no need for a lock.
        return file_type;
    }

    void setLastAccessTime(std::chrono::time_point<std::chrono::system_clock> t)
    {
        std::lock_guard lock(mtx);
        last_access_time = t;
    }

    bool isRecentlyAccess(std::chrono::seconds sec) const
    {
        std::lock_guard lock(mtx);
        return (std::chrono::system_clock::now() - last_access_time) < sec;
    }

    Status getStatus() const
    {
        std::lock_guard lock(mtx);
        return status;
    }

private:
    mutable std::mutex mtx;
    const String local_fname;
    Status status;
    UInt64 size;
    const FileType file_type;
    std::chrono::time_point<std::chrono::system_clock> last_access_time;
    std::condition_variable cv_ready;
};

using FileSegmentPtr = std::shared_ptr<FileSegment>;

class LRUFileTable
{
public:
    FileSegmentPtr get(const String & key, bool update_lru = true)
    {
        auto itr = table.find(key);
        if (itr == table.end())
        {
            return nullptr;
        }
        auto & [file_seg, lru_itr] = itr->second;
        if (update_lru)
        {
            // Move the key to the end of the queue. The iterator remains valid.
            lru_queue.splice(lru_queue.end(), lru_queue, lru_itr);
        }
        return file_seg;
    }

    void set(const String & key, const FileSegmentPtr & value)
    {
        auto [itr, inserted] = table.emplace(key, std::pair{value, std::list<String>::iterator{}});
        if (inserted)
        {
            itr->second.second = lru_queue.insert(lru_queue.end(), key);
        }
        else
        {
            lru_queue.splice(lru_queue.end(), lru_queue, itr->second.second);
        }
    }

    std::list<String>::iterator begin() { return lru_queue.begin(); }

    std::list<String>::iterator end() { return lru_queue.end(); }

    std::list<String>::iterator remove(const String & key)
    {
        auto itr = table.find(key);
        if (itr == table.end())
        {
            return end();
        }
        auto next_itr = lru_queue.erase(itr->second.second);
        table.erase(itr);
        return next_itr;
    }

    std::vector<FileSegmentPtr> getAllFiles() const
    {
        std::vector<FileSegmentPtr> files;
        for (const auto & pa : table)
        {
            files.push_back(pa.second.first);
        }
        return files;
    }

    size_t size() const { return table.size(); }

private:
    std::list<String> lru_queue;
    std::unordered_map<String, std::pair<FileSegmentPtr, std::list<String>::iterator>> table;
};

class FileCache
{
public:
    static void initialize(PathCapacityMetricsPtr capacity_metrics_, const StorageRemoteCacheConfig & config_)
    {
        global_file_cache_instance = std::make_unique<FileCache>(capacity_metrics_, config_);
        global_file_cache_initialized.store(true, std::memory_order_release);
    }

    static FileCache * instance()
    {
        return global_file_cache_initialized.load(std::memory_order_acquire) ? global_file_cache_instance.get()
                                                                             : nullptr;
    }

    static void shutdown() { global_file_cache_instance = nullptr; }

    FileCache(PathCapacityMetricsPtr capacity_metrics_, const StorageRemoteCacheConfig & config_);

    RandomAccessFilePtr getRandomAccessFile(
        const S3::S3FilenameView & s3_fname,
        const std::optional<UInt64> & filesize);

    /// Download the file if it is not in the local cache and returns the
    /// file guard of the local cache file. When file guard is alive,
    /// local file will not be evicted.
    FileSegmentPtr downloadFileForLocalRead(
        const S3::S3FilenameView & s3_fname,
        const std::optional<UInt64> & filesize);

    void updateConfig(const Settings & settings);

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif

    inline static std::atomic<bool> global_file_cache_initialized{false};
    static std::unique_ptr<FileCache> global_file_cache_instance;

    DISALLOW_COPY_AND_MOVE(FileCache);

    FileSegmentPtr get(const S3::S3FilenameView & s3_fname, const std::optional<UInt64> & filesize = std::nullopt);
    /// Try best to wait until the file is available in cache. If the file is not in cache, it will download the file in foreground.
    /// It may return nullptr after wait. In this case the caller could retry.
    FileSegmentPtr getOrWait(
        const S3::S3FilenameView & s3_fname,
        const std::optional<UInt64> & filesize = std::nullopt);

    void bgDownload(const String & s3_key, FileSegmentPtr & file_seg);
    void fgDownload(std::unique_lock<std::mutex> & cache_lock, const String & s3_key, FileSegmentPtr & file_seg);
    void download(const String & s3_key, FileSegmentPtr & file_seg);
    void downloadImpl(const String & s3_key, FileSegmentPtr & file_seg);

    static String toTemporaryFilename(const String & fname);
    static bool isTemporaryFilename(const String & fname);
    static void prepareDir(const String & dir_name);
    static void prepareParentDir(const String & local_fname);
    static bool isS3Filename(const String & fname);
    String toLocalFilename(const String & s3_key);
    String toS3Key(const String & local_fname);

    void restore();
    void restoreWriteNode(const std::filesystem::directory_entry & write_node_entry);
    void restoreTable(const std::filesystem::directory_entry & table_entry);
    void restoreDMFile(const std::filesystem::directory_entry & dmfile_entry);

    void remove(std::unique_lock<std::mutex> & cache_lock, const String & s3_key, bool force = false);
    void remove(const String & s3_key, bool force = false);
    std::pair<Int64, std::list<String>::iterator> removeImpl(
        LRUFileTable & table,
        const String & s3_key,
        FileSegmentPtr & f,
        bool force = false);
    void removeDiskFile(const String & local_fname, bool update_fsize_metrics) const;

    // Estimated size is an empirical value.
    // We don't know object size before get object from S3.
    // But we need reserve space for a object before download it
    // to avoid wasting request to S3 if cache capacity is exhausted.
    // TODO: The size of most objects, such as size and data type, can be parsed from the metadata file of DMFile.
    // We can try to pass this information, although it may be troublesome.
    static constexpr UInt64 estimated_size_of_file_type[] = {
        0, // Unknow type, currently never cache it.
        8 * 1024, // Estimated size of meta.
        1 * 1024 * 1024, // Estimated size of merged.
        8 * 1024, // Estimated size of index.
        8 * 1024, // Estimated size of mark.
        8 * 1024, // Estimated size of null map.
        8 * 1024, // Estimated size of delete mark column.
        50 * 1024, // Estimated size of version column.
        5 * 1024 * 1024, // Estimated size of handle/version/delete mark.
        12 * 1024 * 1024, // Estimated size of other data columns.
    };
    static_assert(
        sizeof(estimated_size_of_file_type) / sizeof(estimated_size_of_file_type[0])
        == magic_enum::enum_count<FileSegment::FileType>());
    static UInt64 getEstimatedSizeOfFileType(FileSegment::FileType file_type);
    static FileSegment::FileType getFileType(const String & fname);
    static FileSegment::FileType getFileTypeOfColData(const std::filesystem::path & p);
    bool canCache(FileSegment::FileType file_type) const;
    bool reserveSpaceImpl(FileSegment::FileType reserve_for, UInt64 size, bool try_evict);
    void releaseSpaceImpl(UInt64 size);
    void releaseSpace(UInt64 size);
    bool reserveSpace(FileSegment::FileType reserve_for, UInt64 size, bool try_evict);
    bool finalizeReservedSize(FileSegment::FileType reserve_for, UInt64 reserved_size, UInt64 content_length);
    static std::vector<FileSegment::FileType> getEvictFileTypes(FileSegment::FileType evict_for);
    void tryEvictFile(FileSegment::FileType evict_for, UInt64 size);
    UInt64 tryEvictFrom(FileSegment::FileType evict_for, UInt64 size, FileSegment::FileType evict_from);

    // This function is used for test.
    std::vector<FileSegmentPtr> getAll();

    std::mutex mtx;
    PathCapacityMetricsPtr capacity_metrics;
    String cache_dir;
    UInt64 cache_capacity;
    UInt64 cache_level;
    UInt64 cache_used;
    std::atomic<UInt64> cache_min_age_seconds = 1800;
    std::atomic<double> max_downloading_count_scale = 1.0;
    std::array<LRUFileTable, magic_enum::enum_count<FileSegment::FileType>()> tables;

    // Currently, these variables are just use for testing.
    std::atomic<UInt64> bg_downloading_count = 0;
    std::atomic<UInt64> bg_download_succ_count = 0;
    std::atomic<UInt64> bg_download_fail_count = 0;

    DB::LoggerPtr log;
};
} // namespace DB

// Make std::filesystem::path formattable.
template <>
struct fmt::formatter<std::filesystem::path> : formatter<std::string_view>
{
    template <typename FormatContext>
    auto format(const std::filesystem::path & path, FormatContext & ctx)
    {
        return formatter<std::string_view>::format(path.string(), ctx);
    }
};
