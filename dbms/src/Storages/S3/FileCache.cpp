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
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Common/escapeForFileName.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Settings.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>

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

namespace DB
{
using FileType = FileSegment::FileType;

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
    if (!reserveSpaceImpl(file_type, estimzted_size, /*try_evict*/ true))
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

    std::lock_guard lock(mtx);
    auto f = table.get(s3_key, /*update_lru*/ false);
    if (f == nullptr)
    {
        return;
    }
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

bool FileCache::reserveSpaceImpl(FileType reserve_for, UInt64 size, bool try_evict)
{
    if (cache_used + size <= cache_capacity)
    {
        cache_used += size;
        CurrentMetrics::set(CurrentMetrics::DTFileCacheUsed, cache_used);
        return true;
    }
    if (try_evict)
    {
        UInt64 min_evict_size = size - (cache_capacity - cache_used);
        LOG_DEBUG(log, "tryEvictFile for {} min_evict_size={}", magic_enum::enum_name(reserve_for), min_evict_size);
        tryEvictFile(reserve_for, min_evict_size);
        return reserveSpaceImpl(reserve_for, size, /*try_evict*/ false);
    }
    return false;
}

// The basic evict logic:
// Distinguish cache priority according to file type. The larger the file type, the lower the priority.
// First, try to evict files which not be used recently with the same type. => Try to evict old files.
// Second, try to evict files with lower priority. => Try to evict lower priority files.
std::vector<FileType> FileCache::getEvictFileTypes(FileType evict_for)
{
    std::vector<FileType> evict_types;
    evict_types.push_back(evict_for); // First, try evict with the same file type.
    constexpr auto all_file_types = magic_enum::enum_values<FileType>(); // all_file_types are sorted by enum value.
    // Second, try evict from the lower proirity file type.
    for (auto itr = std::rbegin(all_file_types); itr != std::rend(all_file_types) && *itr > evict_for; ++itr)
    {
        evict_types.push_back(*itr);
    }
    return evict_types;
}

void FileCache::tryEvictFile(FileType evict_for, UInt64 size)
{
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
            break;
        }
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

bool FileCache::reserveSpace(FileType reserve_for, UInt64 size, bool try_evict)
{
    std::lock_guard lock(mtx);
    return reserveSpaceImpl(reserve_for, size, try_evict);
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
    if (ext.empty())
    {
        return p.stem() == DM::DMFile::metav2FileName() ? FileType::Meta : FileType::Unknow;
    }
    else if (ext == ".merged")
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
    else
    {
        return FileType::Unknow;
    }
}

bool FileCache::finalizeReservedSize(FileType reserve_for, UInt64 reserved_size, UInt64 content_length)
{
    if (content_length > reserved_size)
    {
        // Need more space.
        return reserveSpace(reserve_for, content_length - reserved_size, /*try_evict*/ true);
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

bool FileCache::isS3Filename(const String & fname)
{
    return S3::S3FilenameView::fromKey(fname).isValid();
}

String FileCache::toLocalFilename(const String & s3_key)
{
    return fmt::format("{}/{}", cache_dir, s3_key);
}

String FileCache::toS3Key(const String & local_fname)
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
