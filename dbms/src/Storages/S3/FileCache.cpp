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

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/escapeForFileName.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Settings.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>
#include <Storages/S3/S3Common.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <magic_enum.hpp>
#include <queue>

namespace ProfileEvents
{
extern const Event S3GetObject;
extern const Event S3ReadBytes;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric DTFileCacheCapacity;
extern const Metric DTFileCacheUsed;
} // namespace CurrentMetrics

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
extern const int FILE_DOESNT_EXIST;
} // namespace DB::ErrorCodes

namespace DB::FailPoints
{
extern const char file_cache_fg_download_fail[];
} // namespace DB::FailPoints

namespace DB
{
using FileType = FileSegment::FileType;

std::unique_ptr<FileCache> FileCache::global_file_cache_instance;

FileSegment::Status FileSegment::waitForNotEmpty()
{
    std::unique_lock lock(mtx);

    if (status != Status::Empty)
        return status;

    PerfContext::file_cache.fg_wait_download_from_s3++;

    Stopwatch watch;

    while (true)
    {
        SYNC_FOR("before_FileSegment::waitForNotEmpty_wait"); // just before actual waiting...

        auto is_done = cv_ready.wait_for(lock, std::chrono::seconds(30), [&] { return status != Status::Empty; });
        if (is_done)
            break;

        double elapsed_secs = watch.elapsedSeconds();
        LOG_WARNING(
            Logger::get(),
            "FileCache is still waiting FileSegment ready, file={} elapsed={}s",
            local_fname,
            elapsed_secs);

        // Snapshot time is 300s
        if (elapsed_secs > 300)
        {
            throw Exception(
                ErrorCodes::S3_ERROR,
                "Failed to wait until S3 file {} is ready after {}s",
                local_fname,
                elapsed_secs);
        }
    }

    return status;
}

FileCache::FileCache(PathCapacityMetricsPtr capacity_metrics_, const StorageRemoteCacheConfig & config_)
    : capacity_metrics(capacity_metrics_)
    , cache_dir(config_.getDTFileCacheDir())
    , cache_capacity(config_.getDTFileCapacity())
    , cache_level(config_.dtfile_level)
    , cache_used(0)
    , log(Logger::get("FileCache"))
{
    CurrentMetrics::set(CurrentMetrics::DTFileCacheCapacity, cache_capacity);
    prepareDir(cache_dir);
    restore();
}

RandomAccessFilePtr FileCache::getRandomAccessFile(
    const S3::S3FilenameView & s3_fname,
    const std::optional<UInt64> & filesize)
{
    auto file_seg = get(s3_fname, filesize);
    if (file_seg == nullptr)
    {
        return nullptr;
    }
    try
    {
        // PosixRandomAccessFile should hold the `file_seg` shared_ptr to prevent cached file from evicted.
        return std::make_shared<PosixRandomAccessFile>(
            file_seg->getLocalFileName(),
            /*flags*/ -1,
            /*read_limiter*/ nullptr,
            file_seg);
    }
    catch (const DB::Exception & e)
    {
        LOG_WARNING(
            log,
            "s3_fname={} local_fname={} status={} errcode={} errmsg={}",
            s3_fname.toFullKey(),
            file_seg->getLocalFileName(),
            magic_enum::enum_name(file_seg->getStatus()),
            e.code(),
            e.message());
        if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
        {
            // Normally, this would not happen. But if someone removes cache files manually, the status of memory and filesystem are inconsistent.
            // We can handle this situation by remove it from FileCache.
            remove(s3_fname.toFullKey(), /*force*/ true);
        }
        throw; // Rethrow currently exception, caller should handle it.
    }
}

FileSegmentPtr FileCache::downloadFileForLocalRead(
    const S3::S3FilenameView & s3_fname,
    const std::optional<UInt64> & filesize)
{
    auto file_seg = getOrWait(s3_fname, filesize);
    if (!file_seg)
        return nullptr;

    auto path = file_seg->getLocalFileName();
    if likely (Poco::File(path).exists())
        return file_seg;

    // Normally, this would not happen. But if someone removes cache files manually, the status of memory and filesystem are inconsistent.
    // We can handle this situation by remove it from FileCache.
    remove(s3_fname.toFullKey(), /*force*/ true);
    return nullptr;
}

FileSegmentPtr FileCache::get(const S3::S3FilenameView & s3_fname, const std::optional<UInt64> & filesize)
{
    auto s3_key = s3_fname.toFullKey();
    auto file_type = getFileType(s3_key);
    auto & table = tables[static_cast<UInt64>(file_type)];

    std::lock_guard lock(mtx);

    auto f = table.get(s3_key);
    if (f != nullptr)
    {
        f->setLastAccessTime(std::chrono::system_clock::now());
        if (f->isReadyToRead())
        {
            GET_METRIC(tiflash_storage_remote_cache, type_dtfile_hit).Increment();
            return f;
        }
        else
        {
            GET_METRIC(tiflash_storage_remote_cache, type_dtfile_miss).Increment();
            return nullptr;
        }
    }

    GET_METRIC(tiflash_storage_remote_cache, type_dtfile_miss).Increment();
    if (!canCache(file_type))
    {
        // Don't cache this file type or too many downloading task.
        return nullptr;
    }

    // File not exists, try to download and cache it in backgroud.

    // We don't know the exact size of a object/file, but we need reserve space to save the object/file.
    // A certain amount of space is reserved for each file type.
    auto estimzted_size = filesize ? *filesize : getEstimatedSizeOfFileType(file_type);
    if (!reserveSpaceImpl(file_type, estimzted_size, EvictMode::TryEvict))
    {
        // Space not enough.
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_full).Increment();
        LOG_DEBUG(
            log,
            "s3_key={} space not enough(capacity={} used={} estimzted_size={}), skip cache",
            s3_key,
            cache_capacity,
            cache_used,
            estimzted_size);
        return nullptr;
    }

    auto file_seg
        = std::make_shared<FileSegment>(toLocalFilename(s3_key), FileSegment::Status::Empty, estimzted_size, file_type);
    table.set(s3_key, file_seg);
    bgDownload(s3_key, file_seg);

    return nullptr;
}

FileSegmentPtr FileCache::getOrWait(const S3::S3FilenameView & s3_fname, const std::optional<UInt64> & filesize)
{
    auto s3_key = s3_fname.toFullKey();
    auto file_type = getFileType(s3_key);
    auto & table = tables[static_cast<UInt64>(file_type)];

    std::unique_lock lock(mtx);

    auto f = table.get(s3_key);
    if (f != nullptr)
    {
        lock.unlock();
        f->setLastAccessTime(std::chrono::system_clock::now());
        auto status = f->waitForNotEmpty();
        if (status == FileSegment::Status::Complete)
        {
            GET_METRIC(tiflash_storage_remote_cache, type_dtfile_hit).Increment();
            return f;
        }
        // On-going download failed, let the caller retry.
        return nullptr;
    }

    GET_METRIC(tiflash_storage_remote_cache, type_dtfile_miss).Increment();

    auto estimated_size = filesize ? *filesize : getEstimatedSizeOfFileType(file_type);
    if (!reserveSpaceImpl(file_type, estimated_size, EvictMode::ForceEvict))
    {
        // Space not enough.
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_full).Increment();
        LOG_INFO(
            log,
            "s3_key={} space not enough(capacity={} used={} estimzted_size={}), skip cache",
            s3_key,
            cache_capacity,
            cache_used,
            estimated_size);

        // Just throw, no need to let the caller retry.
        throw Exception(ErrorCodes::S3_ERROR, "Cannot reserve {} space for object {}", estimated_size, s3_key);
    }

    auto file_seg
        = std::make_shared<FileSegment>(toLocalFilename(s3_key), FileSegment::Status::Empty, estimated_size, file_type);
    table.set(s3_key, file_seg);
    lock.unlock();

    ++PerfContext::file_cache.fg_download_from_s3;
    fgDownload(s3_key, file_seg);
    if (!file_seg || !file_seg->isReadyToRead())
        throw Exception(ErrorCodes::S3_ERROR, "Download object {} failed", s3_key);

    return file_seg;
}

// Remove `local_fname` from disk and remove parent directory if parent directory is empty.
void FileCache::removeDiskFile(const String & local_fname, bool update_fsize_metrics) const
{
    if (!std::filesystem::exists(local_fname))
    {
        return;
    }
    try
    {
        auto fsize = std::filesystem::file_size(local_fname);
        for (std::filesystem::path p(local_fname); p.is_absolute() && std::filesystem::exists(p); p = p.parent_path())
        {
            auto s = p.string();
            if (s != cache_dir && (s == local_fname || std::filesystem::is_empty(p)))
            {
                std::filesystem::remove(p); // If p is a directory, remove success only when it is empty.
                if (s == local_fname && update_fsize_metrics)
                {
                    capacity_metrics->freeUsedSize(local_fname, fsize);
                }
            }
            else
            {
                break;
            }
        }
    }
    catch (std::exception & e)
    {
        LOG_WARNING(log, "Throw exception in removeFile {}: {}", local_fname, e.what());
    }
    catch (...)
    {
        tryLogCurrentException("Throw exception in removeFile {}", local_fname);
    }
}

void FileCache::remove(const String & s3_key, bool force)
{
    auto file_type = getFileType(s3_key);
    auto & table = tables[static_cast<UInt64>(file_type)];

    std::unique_lock lock(mtx);
    auto f = table.get(s3_key, /*update_lru*/ false);
    if (f == nullptr)
        return;
    std::ignore = removeImpl(table, s3_key, f, force);
}

std::pair<Int64, std::list<String>::iterator> FileCache::removeImpl(
    LRUFileTable & table,
    const String & s3_key,
    FileSegmentPtr & f,
    bool force)
{
    // Except currenly thread and the FileTable,
    // there are other threads hold this FileSegment object.
    if (f.use_count() > 2 && !force)
    {
        return {-1, {}};
    }
    const auto & local_fname = f->getLocalFileName();
    removeDiskFile(local_fname, /*update_fsize_metrics*/ true);
    auto temp_fname = toTemporaryFilename(local_fname);
    // Not update fsize metrics for temporary files because they are not add to fsize metrics before.
    removeDiskFile(temp_fname, /*update_fsize_metrics*/ false);

    auto release_size = f->getSize();
    GET_METRIC(tiflash_storage_remote_cache, type_dtfile_evict).Increment();
    GET_METRIC(tiflash_storage_remote_cache_bytes, type_dtfile_evict_bytes).Increment(release_size);
    releaseSpaceImpl(release_size);
    return {release_size, table.remove(s3_key)};
}

bool FileCache::reserveSpaceImpl(FileType reserve_for, UInt64 size, EvictMode evict)
{
    if (cache_used + size <= cache_capacity)
    {
        cache_used += size;
        CurrentMetrics::set(CurrentMetrics::DTFileCacheUsed, cache_used);
        return true;
    }
    if (evict == EvictMode::TryEvict || evict == EvictMode::ForceEvict)
    {
        UInt64 min_evict_size = size - (cache_capacity - cache_used);
        LOG_DEBUG(
            log,
            "tryEvictFile for {} min_evict_size={} evict_mode={}",
            magic_enum::enum_name(reserve_for),
            min_evict_size,
            magic_enum::enum_name(evict));
        tryEvictFile(reserve_for, min_evict_size, evict);
        return reserveSpaceImpl(reserve_for, size, EvictMode::NoEvict);
    }
    return false;
}

// The basic evict logic:
// Distinguish cache priority according to file type. The larger the file type, the lower the priority.
// First, try to evict files which not be used recently with the same type. => Try to evict old files.
// Second, try to evict files with lower priority. => Try to evict lower priority files.
// Finally, evict files with higher priority, if space is still not sufficient. Higher priority files
// are usually smaller. If we don't evict them, it is very possible that cache is full of these higher
// priority small files and we can't effectively cache any lower-priority large files.
std::vector<FileType> FileCache::getEvictFileTypes(FileType evict_for)
{
    std::vector<FileType> evict_types;
    evict_types.push_back(evict_for); // First, try evict with the same file type.
    constexpr auto all_file_types = magic_enum::enum_values<FileType>(); // all_file_types are sorted by enum value.
    // Second, try evict from the lower proirity file type.
    for (auto itr = std::rbegin(all_file_types); itr != std::rend(all_file_types); ++itr)
    {
        if (*itr != evict_for)
            evict_types.push_back(*itr);
    }
    return evict_types;
}

void FileCache::tryEvictFile(FileType evict_for, UInt64 size, EvictMode evict)
{
    RUNTIME_CHECK(evict != EvictMode::NoEvict);

    auto file_types = getEvictFileTypes(evict_for);
    for (auto evict_from : file_types)
    {
        auto evicted_size = tryEvictFrom(evict_for, size, evict_from);
        LOG_DEBUG(
            log,
            "tryEvictFrom {} required_size={} evicted_size={}",
            magic_enum::enum_name(evict_from),
            size,
            evicted_size);
        if (size > evicted_size)
        {
            size -= evicted_size;
        }
        else
        {
            size = 0;
            break;
        }
    }

    if (size > 0 && evict == EvictMode::ForceEvict)
    {
        // After a series of tryEvict, the space is still not sufficient,
        // so we do a force eviction.
        auto evicted_size = forceEvict(size);
        LOG_DEBUG(log, "forceEvict required_size={} evicted_size={}", size, evicted_size);
    }
}

UInt64 FileCache::tryEvictFrom(FileType evict_for, UInt64 size, FileType evict_from)
{
    auto & table = tables[static_cast<UInt64>(evict_from)];
    UInt64 total_released_size = 0;
    constexpr UInt32 max_try_evict_count = 10;
    // File type that we evict for does not have higher priority,
    // so we need check last access time to prevent recently used files from evicting.
    bool check_last_access_time = evict_for >= evict_from;
    auto itr = table.begin();
    auto end = table.end();
    for (UInt32 try_evict_count = 0; try_evict_count < max_try_evict_count && itr != end; ++try_evict_count)
    {
        auto s3_key = *itr;
        auto f = table.get(s3_key, /*update_lru*/ false);
        if (!check_last_access_time
            || !f->isRecentlyAccess(std::chrono::seconds(cache_min_age_seconds.load(std::memory_order_relaxed))))
        {
            auto [released_size, next_itr] = removeImpl(table, s3_key, f);
            LOG_DEBUG(log, "tryRemoveFile {} size={}", s3_key, released_size);
            if (released_size < 0) // not remove
            {
                ++itr;
            }
            else // removed
            {
                itr = next_itr;
                total_released_size += released_size;
            }
        }
        else
        {
            ++itr;
        }
        if (total_released_size >= size || try_evict_count >= max_try_evict_count)
        {
            break;
        }
    }
    return total_released_size;
}

struct ForceEvictCandidate
{
    UInt64 file_type_slot;
    String s3_key;
    FileSegmentPtr file_segment;
    std::chrono::time_point<std::chrono::system_clock> last_access_time; // Order by this field
};

struct ForceEvictCandidateComparer
{
    bool operator()(ForceEvictCandidate a, ForceEvictCandidate b) { return a.last_access_time > b.last_access_time; }
};

UInt64 FileCache::forceEvict(UInt64 size_to_evict)
{
    if (size_to_evict == 0)
        return 0;

    // For a force evict, we simply evict from the oldest to the newest, until
    // space is sufficient.

    std::priority_queue<ForceEvictCandidate, std::vector<ForceEvictCandidate>, ForceEvictCandidateComparer>
        evict_candidates;

    // First, pick an item from all levels.

    size_t total_released_size = 0;

    constexpr auto all_file_types = magic_enum::enum_values<FileType>();
    std::vector<std::list<String>::iterator> each_type_lru_iters; // Stores the iterator of next candicate to add
    each_type_lru_iters.reserve(all_file_types.size());
    for (const auto file_type : all_file_types)
    {
        auto file_type_slot = static_cast<UInt64>(file_type);
        auto iter = tables[file_type_slot].begin();
        if (iter != tables[file_type_slot].end())
        {
            const auto & s3_key = *iter;
            const auto & f = tables[file_type_slot].get(s3_key, /*update_lru*/ false);
            evict_candidates.emplace(ForceEvictCandidate{
                .file_type_slot = file_type_slot,
                .s3_key = s3_key,
                .file_segment = f,
                .last_access_time = f->getLastAccessTime(),
            });
            iter++;
        }
        each_type_lru_iters.emplace_back(iter);
    }

    // Then we iterate the heap to remove the file with oldest access time.

    while (!evict_candidates.empty())
    {
        auto to_evict = evict_candidates.top(); // intentionally copy
        evict_candidates.pop();

        const auto file_type_slot = to_evict.file_type_slot;
        if (each_type_lru_iters[file_type_slot] != tables[file_type_slot].end())
        {
            const auto s3_key = *each_type_lru_iters[file_type_slot];
            const auto & f = tables[file_type_slot].get(s3_key, /*update_lru*/ false);
            evict_candidates.emplace(ForceEvictCandidate{
                .file_type_slot = file_type_slot,
                .s3_key = s3_key,
                .file_segment = f,
                .last_access_time = f->getLastAccessTime(),
            });
            each_type_lru_iters[file_type_slot]++;
        }

        auto [released_size, next_itr] = removeImpl(tables[file_type_slot], to_evict.s3_key, to_evict.file_segment);
        LOG_DEBUG(log, "ForceEvict {} size={}", to_evict.s3_key, released_size);
        if (released_size >= 0) // removed
        {
            total_released_size += released_size;
            if (total_released_size >= size_to_evict)
                break;
        }
    }
    return total_released_size;
}

bool FileCache::reserveSpace(FileType reserve_for, UInt64 size, EvictMode evict)
{
    std::lock_guard lock(mtx);
    return reserveSpaceImpl(reserve_for, size, evict);
}

void FileCache::releaseSpaceImpl(UInt64 size)
{
    cache_used -= size;
    CurrentMetrics::set(CurrentMetrics::DTFileCacheUsed, cache_used);
}

void FileCache::releaseSpace(UInt64 size)
{
    std::lock_guard lock(mtx);
    releaseSpaceImpl(size);
}

bool FileCache::canCache(FileType file_type) const
{
    return file_type != FileType::Unknow && static_cast<UInt64>(file_type) <= cache_level
        && bg_downloading_count.load(std::memory_order_relaxed)
        < S3FileCachePool::get().getMaxThreads() * max_downloading_count_scale.load(std::memory_order_relaxed);
}

FileType FileCache::getFileTypeOfColData(const std::filesystem::path & p)
{
    if (p.extension() == ".null")
    {
        // .null.dat
        return FileType::NullMap;
    }
    auto str_col_id = unescapeForFileName(p.stem().string());
    auto col_id = std::stol(str_col_id);
    switch (col_id)
    {
    case EXTRA_HANDLE_COLUMN_ID:
        return FileType::HandleColData;
    case VERSION_COLUMN_ID:
        return FileType::VersionColData;
    case TAG_COLUMN_ID:
        return FileType::DeleteMarkColData;
    default:
        return FileType::ColData;
    }
}

UInt64 FileCache::getEstimatedSizeOfFileType(FileSegment::FileType file_type)
{
    return estimated_size_of_file_type[static_cast<UInt64>(file_type)];
}

FileType FileCache::getFileType(const String & fname)
{
    std::filesystem::path p(fname);

    auto ext = p.extension();
    if (ext == ".merged")
    {
        return FileType::Merged;
    }
    else if (ext == ".idx")
    {
        return FileType::Index;
    }
    else if (ext == ".mrk")
    {
        return FileType::Mark;
    }
    else if (ext == ".dat")
    {
        return getFileTypeOfColData(p.stem());
    }
    else if (ext == ".meta")
    {
        // Example: v1.meta
        return FileType::Meta;
    }
    else if (ext.empty() && p.stem() == "meta")
    {
        return FileType::Meta;
    }

    return FileType::Unknow;
}

bool FileCache::finalizeReservedSize(FileType reserve_for, UInt64 reserved_size, UInt64 content_length)
{
    if (content_length > reserved_size)
    {
        // Need more space.
        return reserveSpace(reserve_for, content_length - reserved_size, EvictMode::TryEvict);
    }
    else if (content_length < reserved_size)
    {
        // Release extra space.
        releaseSpace(reserved_size - content_length);
    }
    return true;
}

void FileCache::downloadImpl(const String & s3_key, FileSegmentPtr & file_seg)
{
    Stopwatch sw;
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    Aws::S3::Model::GetObjectRequest req;
    client->setBucketAndKeyWithRoot(req, s3_key);
    ProfileEvents::increment(ProfileEvents::S3GetObject);
    auto outcome = client->GetObject(req);
    if (!outcome.IsSuccess())
    {
        throw S3::fromS3Error(outcome.GetError(), "s3_key={}", s3_key);
    }
    auto & result = outcome.GetResult();
    auto content_length = result.GetContentLength();
    RUNTIME_CHECK(content_length >= 0, s3_key, content_length);
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, content_length);
    GET_METRIC(tiflash_storage_s3_request_seconds, type_get_object).Observe(sw.elapsedSeconds());
    if (!finalizeReservedSize(file_seg->getFileType(), file_seg->getSize(), content_length))
    {
        LOG_DEBUG(log, "s3_key={} finalizeReservedSize {}=>{} failed.", s3_key, file_seg->getSize(), content_length);
        return;
    }
    file_seg->setSize(content_length);

    const auto & local_fname = file_seg->getLocalFileName();
    prepareParentDir(local_fname);
    auto temp_fname = toTemporaryFilename(local_fname);
    {
        Aws::OFStream ostr(temp_fname, std::ios_base::out | std::ios_base::binary);
        RUNTIME_CHECK_MSG(ostr.is_open(), "Open {} failed: {}", temp_fname, strerror(errno));
        if (content_length > 0)
        {
            GET_METRIC(tiflash_storage_remote_cache_bytes, type_dtfile_download_bytes).Increment(content_length);
            ostr << result.GetBody().rdbuf();
            // If content_length == 0, ostr.good() is false. Does not know the reason.
            RUNTIME_CHECK_MSG(
                ostr.good(),
                "Write {} content_length {} failed: {}",
                temp_fname,
                content_length,
                strerror(errno));
            ostr.flush();
        }
    }
    std::filesystem::rename(temp_fname, local_fname);
    auto fsize = std::filesystem::file_size(local_fname);
    capacity_metrics->addUsedSize(local_fname, fsize);
    RUNTIME_CHECK_MSG(
        fsize == static_cast<UInt64>(content_length),
        "local_fname={}, file_size={}, content_length={}",
        local_fname,
        fsize,
        content_length);
    file_seg->setStatus(FileSegment::Status::Complete);
    LOG_DEBUG(
        log,
        "Download s3_key={} to local={} size={} cost={}ms",
        s3_key,
        local_fname,
        content_length,
        sw.elapsedMilliseconds());
}

void FileCache::download(const String & s3_key, FileSegmentPtr & file_seg)
{
    try
    {
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_download).Increment();
        downloadImpl(s3_key, file_seg);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Download s3_key={} failed", s3_key));
    }

    if (!file_seg->isReadyToRead())
    {
        file_seg->setStatus(FileSegment::Status::Failed);
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_download_failed).Increment();
        bg_download_fail_count.fetch_add(1, std::memory_order_relaxed);
        file_seg.reset();
        remove(s3_key);
    }
    else
    {
        bg_download_succ_count.fetch_add(1, std::memory_order_relaxed);
    }
    bg_downloading_count.fetch_sub(1, std::memory_order_relaxed);
    LOG_DEBUG(
        log,
        "downloading count {} => s3_key {} finished",
        bg_downloading_count.load(std::memory_order_relaxed),
        s3_key);
}

void FileCache::bgDownload(const String & s3_key, FileSegmentPtr & file_seg)
{
    bg_downloading_count.fetch_add(1, std::memory_order_relaxed);
    LOG_DEBUG(
        log,
        "downloading count {} => s3_key {} start",
        bg_downloading_count.load(std::memory_order_relaxed),
        s3_key);
    S3FileCachePool::get().scheduleOrThrowOnError(
        [this, s3_key = s3_key, file_seg = file_seg]() mutable { download(s3_key, file_seg); });
}

void FileCache::fgDownload(const String & s3_key, FileSegmentPtr & file_seg)
{
    SYNC_FOR("FileCache::fgDownload"); // simulate long s3 download

    try
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::file_cache_fg_download_fail);
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_download).Increment();
        downloadImpl(s3_key, file_seg);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Download s3_key={} failed", s3_key));
    }

    if (!file_seg->isReadyToRead())
    {
        file_seg->setStatus(FileSegment::Status::Failed);
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_download_failed).Increment();
        file_seg.reset();
        remove(s3_key);
    }

    LOG_DEBUG(log, "foreground downloading => s3_key {} finished", s3_key);
}

bool FileCache::isS3Filename(const String & fname)
{
    return S3::S3FilenameView::fromKey(fname).isValid();
}

String FileCache::toLocalFilename(const String & s3_key)
{
    return fmt::format("{}/{}", cache_dir, s3_key);
}

String FileCache::toS3Key(const String & local_fname) const
{
    return local_fname.substr(cache_dir.size() + 1);
}

String FileCache::toTemporaryFilename(const String & fname)
{
    std::filesystem::path p(fname);
    if (p.extension() != ".tmp")
    {
        return fmt::format("{}.tmp", fname);
    }
    else
    {
        return fname;
    }
}

bool FileCache::isTemporaryFilename(const String & fname)
{
    std::filesystem::path p(fname);
    return p.extension() == ".tmp";
}

void FileCache::prepareDir(const String & dir)
{
    std::filesystem::path p(dir);
    RUNTIME_CHECK_MSG(
        !std::filesystem::exists(p) || std::filesystem::is_directory(p),
        "{} exists but not directory",
        dir);
    if (!std::filesystem::exists(p))
    {
        std::filesystem::create_directories(p);
    }
}

void FileCache::prepareParentDir(const String & local_fname)
{
    auto pos = local_fname.find_last_of('/');
    RUNTIME_CHECK_MSG(pos != std::string::npos, "local filename {} is invalid", local_fname);
    auto parent_dir = local_fname.substr(0, pos);
    prepareDir(parent_dir);
}

void FileCache::restore()
{
    Stopwatch sw;
    for (const auto & wn_entry : std::filesystem::directory_iterator(cache_dir))
    {
        restoreWriteNode(wn_entry);
    }

    size_t total_count = 0;
    for (const auto & t : tables)
    {
        total_count += t.size();
    }
    LOG_INFO(
        log,
        "restore: cost={:.3f}s used={} capacity={} total_count={}",
        sw.elapsedSeconds(),
        cache_used,
        cache_capacity,
        total_count);
}

void FileCache::restoreWriteNode(const std::filesystem::directory_entry & write_node_entry)
{
    RUNTIME_CHECK_MSG(write_node_entry.is_directory(), "{} is not a directory", write_node_entry.path());
    auto write_node_data_path = write_node_entry.path() / "data";
    if (!std::filesystem::exists(write_node_data_path))
        return;
    for (const auto & table_entry : std::filesystem::directory_iterator(write_node_data_path))
    {
        restoreTable(table_entry);
    }
}

void FileCache::restoreTable(const std::filesystem::directory_entry & table_entry)
{
    RUNTIME_CHECK_MSG(table_entry.is_directory(), "{} is not a directory", table_entry.path());
    for (const auto & dmfile_entry : std::filesystem::directory_iterator(table_entry.path()))
    {
        restoreDMFile(dmfile_entry);
    }
}

void FileCache::restoreDMFile(const std::filesystem::directory_entry & dmfile_entry)
{
    RUNTIME_CHECK_MSG(dmfile_entry.is_directory(), "{} is not a directory", dmfile_entry.path());
    for (const auto & file_entry : std::filesystem::directory_iterator(dmfile_entry.path()))
    {
        RUNTIME_CHECK_MSG(file_entry.is_regular_file(), "{} is not a regular file", file_entry.path());
        auto fname = file_entry.path().string();
        if (unlikely(isTemporaryFilename(fname)))
        {
            // Not update fsize metrics for temporary files because they are not add to fsize metrics before.
            removeDiskFile(fname, /*update_fsize_metrics*/ false);
        }
        else
        {
            auto file_type = getFileType(fname);
            auto & table = tables[static_cast<UInt64>(file_type)];
            auto size = file_entry.file_size();
            if (canCache(file_type) && cache_capacity - cache_used >= size)
            {
                table.set(
                    toS3Key(fname),
                    std::make_shared<FileSegment>(fname, FileSegment::Status::Complete, size, file_type));
                capacity_metrics->addUsedSize(fname, size);
                cache_used += size;
                CurrentMetrics::set(CurrentMetrics::DTFileCacheUsed, cache_used);
            }
            else
            {
                // Not update fsize metrics because this file is not added to metrics yet.
                removeDiskFile(fname, /*update_fsize_metrics*/ false);
            }
        }
    }
}

std::vector<FileSegmentPtr> FileCache::getAll()
{
    std::lock_guard lock(mtx);
    std::vector<FileSegmentPtr> file_segs;
    for (const auto & table : tables)
    {
        auto values = table.getAllFiles();
        file_segs.insert(file_segs.end(), values.begin(), values.end());
    }
    return file_segs;
}

void FileCache::updateConfig(const Settings & settings)
{
    double max_downloading_scale = settings.dt_filecache_max_downloading_count_scale;
    if (std::fabs(max_downloading_scale - max_downloading_count_scale.load(std::memory_order_relaxed)) > 0.001)
    {
        LOG_INFO(
            log,
            "max_downloading_count_scale {} => {}",
            max_downloading_count_scale.load(std::memory_order_relaxed),
            max_downloading_scale);
        max_downloading_count_scale.store(max_downloading_scale, std::memory_order_relaxed);
    }

    UInt64 cache_min_age = settings.dt_filecache_min_age_seconds;
    if (cache_min_age != cache_min_age_seconds.load(std::memory_order_relaxed))
    {
        LOG_INFO(
            log,
            "cache_min_age_seconds {} => {}",
            cache_min_age_seconds.load(std::memory_order_relaxed),
            cache_min_age);
        cache_min_age_seconds.store(cache_min_age, std::memory_order_relaxed);
    }
}

} // namespace DB
