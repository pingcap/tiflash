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
#include <IO/BaseFile/PosixWritableFile.h>
#include <IO/BaseFile/RateLimiter.h>
#include <IO/Buffer/ReadBufferFromIStream.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/IOThreadPools.h>
#include <IO/copyData.h>
#include <Interpreters/Settings.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3ReadLimiter.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <common/logger_useful.h>
#include <fcntl.h>
#include <fmt/chrono.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <magic_enum.hpp>
#include <queue>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#pragma GCC diagnostic pop


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
extern const char file_cache_bg_download_fail[];
} // namespace DB::FailPoints

namespace DB
{
using FileType = FileSegment::FileType;

namespace
{
// A tiny FileCache-only ReadBuffer variant that charges the shared S3 limiter before each refill.
// This lets downloadToLocal keep using the existing copyData/write-buffer path instead of maintaining
// a separate hand-written read/write loop for limiter-enabled downloads.
class ReadBufferFromIStreamWithLimiter : public BufferWithOwnMemory<ReadBuffer>
{
public:
    ReadBufferFromIStreamWithLimiter(
        std::istream & istr_,
        size_t size,
        const std::shared_ptr<S3::S3ReadLimiter> & limiter_,
        S3::S3ReadSource source_)
        : BufferWithOwnMemory<ReadBuffer>(size)
        , istr(istr_)
        , limiter(limiter_)
        , source(source_)
    {}

private:
    bool nextImpl() override
    {
        if (limiter != nullptr)
        {
            // Charge the requested refill size before the actual `istream.read()`. This is intentionally
            // conservative: short reads still spend the full reserved budget for this refill. If we need tighter
            // accounting later, we can extend this path to compensate with the actual bytes read back from S3.
            limiter->requestBytes(internal_buffer.size(), source);
        }

        istr.read(internal_buffer.begin(), internal_buffer.size());
        auto gcount = istr.gcount();
        if (!gcount)
        {
            if (istr.eof())
                return false;
            throw Exception(ErrorCodes::CANNOT_READ_FROM_ISTREAM, "Cannot read from istream");
        }

        working_buffer.resize(gcount);
        return true;
    }

    std::istream & istr;
    std::shared_ptr<S3::S3ReadLimiter> limiter;
    S3::S3ReadSource source;
};

enum class WaitResult
{
    Hit,
    Timeout,
    Failed,
};

enum class BgDownloadStage
{
    QueueWait,
    Download,
};

TiFlashMetrics::RemoteCacheFileTypeMetric toMetricFileType(FileType file_type)
{
    switch (file_type)
    {
    case FileType::Merged:
        return TiFlashMetrics::RemoteCacheFileTypeMetric::Merged;
    case FileType::DeleteMarkColData:
    case FileType::VersionColData:
    case FileType::HandleColData:
    case FileType::ColData:
        return TiFlashMetrics::RemoteCacheFileTypeMetric::ColData;
    default:
        return TiFlashMetrics::RemoteCacheFileTypeMetric::Other;
    }
}

void observeWaitOnDownloadingMetrics(FileType file_type, WaitResult result, UInt64 bytes, double wait_seconds)
{
    GET_METRIC(tiflash_storage_remote_cache, type_wait_on_downloading).Increment();
    auto & metrics = TiFlashMetrics::instance();
    auto metric_file_type = toMetricFileType(file_type);
    switch (result)
    {
    case WaitResult::Hit:
        metrics
            .getRemoteCacheWaitOnDownloadingResultCounter(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Hit)
            .Increment();
        metrics
            .getRemoteCacheWaitOnDownloadingSecondsHistogram(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Hit)
            .Observe(wait_seconds);
        metrics
            .getRemoteCacheWaitOnDownloadingBytesCounter(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Hit)
            .Increment(bytes);
        break;
    case WaitResult::Timeout:
        metrics
            .getRemoteCacheWaitOnDownloadingResultCounter(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Timeout)
            .Increment();
        metrics
            .getRemoteCacheWaitOnDownloadingSecondsHistogram(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Timeout)
            .Observe(wait_seconds);
        metrics
            .getRemoteCacheWaitOnDownloadingBytesCounter(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Timeout)
            .Increment(bytes);
        break;
    case WaitResult::Failed:
        metrics
            .getRemoteCacheWaitOnDownloadingResultCounter(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Failed)
            .Increment();
        metrics
            .getRemoteCacheWaitOnDownloadingSecondsHistogram(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Failed)
            .Observe(wait_seconds);
        metrics
            .getRemoteCacheWaitOnDownloadingBytesCounter(
                metric_file_type,
                TiFlashMetrics::RemoteCacheWaitResultMetric::Failed)
            .Increment(bytes);
        break;
    }

    switch (result)
    {
    case WaitResult::Hit:
        GET_METRIC(tiflash_storage_remote_cache, type_wait_on_downloading_hit).Increment();
        break;
    case WaitResult::Timeout:
        GET_METRIC(tiflash_storage_remote_cache, type_wait_on_downloading_timeout).Increment();
        break;
    case WaitResult::Failed:
        GET_METRIC(tiflash_storage_remote_cache, type_wait_on_downloading_failed).Increment();
        break;
    }
}

void observeRemoteCacheRejectMetrics(FileType file_type)
{
    TiFlashMetrics::instance()
        .getRemoteCacheRejectCounter(
            toMetricFileType(file_type),
            TiFlashMetrics::RemoteCacheRejectReasonMetric::TooManyDownload)
        .Increment();
}

void updateBgDownloadStatusMetrics(Int64 bg_downloading_count)
{
    GET_METRIC(tiflash_storage_remote_cache_status, type_bg_downloading_count).Set(bg_downloading_count);
    const auto running_limit = static_cast<Int64>(S3FileCachePool::get().getMaxThreads());
    GET_METRIC(tiflash_storage_remote_cache_status, type_bg_download_queue_count)
        .Set(std::max<Int64>(0, bg_downloading_count - running_limit));
}

void observeBgDownloadStageMetrics(FileType file_type, BgDownloadStage stage, double seconds)
{
    auto & metrics = TiFlashMetrics::instance();
    auto metric_file_type = toMetricFileType(file_type);
    switch (stage)
    {
    case BgDownloadStage::QueueWait:
        metrics
            .getRemoteCacheBgDownloadStageSecondsHistogram(
                metric_file_type,
                TiFlashMetrics::RemoteCacheDownloadStageMetric::QueueWait)
            .Observe(seconds);
        break;
    case BgDownloadStage::Download:
        metrics
            .getRemoteCacheBgDownloadStageSecondsHistogram(
                metric_file_type,
                TiFlashMetrics::RemoteCacheDownloadStageMetric::Download)
            .Observe(seconds);
        break;
    }
}

} // namespace

std::unique_ptr<FileCache> FileCache::global_file_cache_instance;

FileSegment::Status FileSegment::waitForNotEmpty()
{
    // Foreground callers expect the file to become readable eventually. This path keeps logging
    // slow waits and fails hard after the built-in timeout instead of silently returning `Empty`.
    return waitForNotEmptyImpl(std::nullopt, /*log_progress*/ true, /*throw_on_timeout*/ true);
}

FileSegment::Status FileSegment::waitForNotEmptyFor(std::chrono::milliseconds timeout)
{
    // Bounded-wait callers treat timeout as a normal outcome and will fall back to another path,
    // so this variant waits only once for the specified budget and returns the current status.
    return waitForNotEmptyImpl(timeout, /*log_progress*/ false, /*throw_on_timeout*/ false);
}

FileSegment::Status FileSegment::waitForNotEmptyImpl(
    std::optional<std::chrono::milliseconds> timeout,
    bool log_progress,
    bool throw_on_timeout)
{
    constexpr UInt64 default_wait_log_interval_seconds = 30;
    constexpr UInt64 wait_ready_timeout_seconds = 300;

    std::unique_lock lock(mtx);

    if (status != Status::Empty)
        return status;

    if (log_progress)
        PerfContext::file_cache.fg_wait_download_from_s3++;

    Stopwatch watch;

    while (true)
    {
        auto wait_interval = timeout.value_or(std::chrono::seconds(default_wait_log_interval_seconds));
        SYNC_FOR("before_FileSegment::waitForNotEmpty_wait"); // just before actual waiting...

        auto is_done = cv_ready.wait_for(lock, wait_interval, [&] { return status != Status::Empty; });
        if (is_done)
            break;

        if (timeout.has_value())
            break;

        double elapsed_secs = watch.elapsedSeconds();
        if (log_progress)
        {
            LOG_WARNING(
                Logger::get(),
                "FileCache is still waiting FileSegment ready, file={} elapsed={}s",
                local_fname,
                elapsed_secs);
        }

        // Snapshot time is 300s
        if (throw_on_timeout && elapsed_secs > wait_ready_timeout_seconds)
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

void CacheSizeHistogram::addFileSegment(const FileSegmentPtr & file_seg)
{
    if (!file_seg)
        return;

    auto age = std::chrono::duration_cast<std::chrono::minutes>(
                   std::chrono::system_clock::now() - file_seg->getLastAccessTime())
                   .count();
    UInt64 fsize = file_seg->getSize();
    if (age < 30)
    {
        in30min.count++;
        in30min.bytes += fsize;
    }
    else if (age < 60)
    {
        in60min.count++;
        in60min.bytes += fsize;
    }
    else if (age < 360)
    {
        in360min.count++;
        in360min.bytes += fsize;
    }
    else if (age < 720)
    {
        in720min.count++;
        in720min.bytes += fsize;
    }
    else if (age < 1440)
    {
        in1440min.count++;
        in1440min.bytes += fsize;
    }
    else if (age < 2880)
    {
        in2880min.count++;
        in2880min.bytes += fsize;
    }
    else if (age < 10080)
    {
        in10080min.count++;
        in10080min.bytes += fsize;
    }
    else
    {
        over10080min.count++;
        over10080min.bytes += fsize;
    }
    if (!oldest_access_time || file_seg->getLastAccessTime() < *oldest_access_time)
    {
        oldest_access_time = file_seg->getLastAccessTime();
        oldest_file_size = fsize;
    }
}

Poco::JSON::Object::Ptr CacheSizeHistogram::Stat::toJson() const
{
    if (count == 0)
        return nullptr;
    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object();
    obj->set("count", count);
    obj->set("bytes", bytes);
    return obj;
}

Poco::JSON::Object::Ptr CacheSizeHistogram::toJson() const
{
    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object();
    {
        Poco::JSON::Object::Ptr total = new Poco::JSON::Object();
        total->set(
            "count",
            in30min.count + in60min.count + in360min.count + in720min.count + in1440min.count + in2880min.count
                + in10080min.count + over10080min.count);
        total->set(
            "bytes",
            in30min.bytes + in60min.bytes + in360min.bytes + in720min.bytes + in1440min.bytes + in2880min.bytes
                + in10080min.bytes + over10080min.bytes);
        obj->set("total", total);
    }
    if (auto sub = in30min.toJson(); sub)
        obj->set("in30min", sub);
    if (auto sub = in60min.toJson(); sub)
        obj->set("in60min", sub);
    if (auto sub = in360min.toJson(); sub)
        obj->set("in360min", sub);
    if (auto sub = in720min.toJson(); sub)
        obj->set("in720min", sub);
    if (auto sub = in1440min.toJson(); sub)
        obj->set("in1440min", sub);
    if (auto sub = in2880min.toJson(); sub)
        obj->set("in2880min", sub);
    if (auto sub = in10080min.toJson(); sub)
        obj->set("in10080min", sub);
    if (auto sub = over10080min.toJson(); sub)
        obj->set("over10080min", sub);
    if (oldest_access_time)
    {
        Poco::JSON::Object::Ptr oldest = new Poco::JSON::Object();
        oldest->set("access_time", fmt::format("{:%Y-%m-%d %H:%M:%S}", oldest_access_time.value()));
        oldest->set("size", oldest_file_size);
        obj->set("oldest", oldest);
    }
    return obj;
}

FileCache::FileCache(
    PathCapacityMetricsPtr capacity_metrics_,
    const StorageRemoteCacheConfig & config_,
    UInt16 logical_cores_,
    IORateLimiter & rate_limiter_)
    : capacity_metrics(capacity_metrics_)
    , cache_dir(config_.getDTFileCacheDir())
    , cache_capacity(config_.getDTFileCapacity())
    , cache_level(config_.dtfile_level)
    , cache_used(0)
    , logical_cores(logical_cores_)
    , rate_limiter(rate_limiter_)
    , log(Logger::get("FileCache"))
{
    CurrentMetrics::set(CurrentMetrics::DTFileCacheCapacity, cache_capacity);
    updateBgDownloadStatusMetrics(0);
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

std::tuple<FileSegmentPtr, bool> FileCache::downloadFileForLocalReadWithRetry(
    const S3::S3FilenameView & s3_fname,
    const std::optional<UInt64> & filesize,
    Int32 retry_count)
{
    auto perf_begin = PerfContext::file_cache;
    bool has_s3_download = false;
    for (Int32 i = retry_count; i > 0; --i)
    {
        try
        {
            if (auto seg = downloadFileForLocalRead(s3_fname, filesize); seg)
            {
                if (PerfContext::file_cache.fg_download_from_s3 > perf_begin.fg_download_from_s3 || //
                    PerfContext::file_cache.fg_wait_download_from_s3 > perf_begin.fg_wait_download_from_s3)
                    has_s3_download = true;

                return {seg, has_s3_download};
            }
        }
        catch (...)
        {
            if (i <= 1)
                throw;
        }
    }

    throw Exception(
        ErrorCodes::S3_ERROR,
        "Failed to download S3 file {} after {} retries",
        s3_fname.toFullKey(),
        retry_count);
}

FileSegmentPtr FileCache::get(const S3::S3FilenameView & s3_fname, const std::optional<UInt64> & filesize)
{
    auto s3_key = s3_fname.toFullKey();
    auto file_type = getFileType(s3_key);
    auto & table = tables[static_cast<UInt64>(file_type)];

    FileSegmentPtr file_seg;
    UInt64 wait_ms = 0;
    {
        std::unique_lock lock(mtx);
        if (auto f = table.get(s3_key); f != nullptr)
        {
            f->setLastAccessTime(std::chrono::system_clock::now());
            if (f->isReadyToRead())
            {
                // Hot-cache fast path: the file is already materialized locally, so return the existing segment
                // immediately without touching any download scheduling or bounded-wait logic.
                GET_METRIC(tiflash_storage_remote_cache, type_dtfile_hit).Increment();
                return f;
            }

            // Another thread is already downloading the same object. Optionally wait for a bounded time and
            // reuse that result instead of opening one more `GetObject` stream for the same key.
            wait_ms = wait_on_downloading_ms.load(std::memory_order_relaxed);
            if (wait_ms == 0)
            {
                GET_METRIC(tiflash_storage_remote_cache, type_dtfile_miss).Increment();
                return nullptr;
            }
            file_seg = f;
        }
        else
        {
            GET_METRIC(tiflash_storage_remote_cache, type_dtfile_miss).Increment();
            // Admission control before any reservation work: skip file types that should never enter FileCache,
            // and stop creating new `Empty` placeholders once background downloading is already saturated.
            switch (canCache(file_type))
            {
            case ShouldCacheRes::RejectTypeNotMatch:
                GET_METRIC(tiflash_storage_remote_cache, type_dtfile_not_cache_type).Increment();
                return nullptr;
            case ShouldCacheRes::RejectTooManyDownloading:
                GET_METRIC(tiflash_storage_remote_cache, type_dtfile_too_many_download).Increment();
                observeRemoteCacheRejectMetrics(file_type);
                return nullptr;
            case ShouldCacheRes::Cache:
                break;
            }

            // File not exists, try to download and cache it in background.

            // We don't know the exact size of a object/file, but we need reserve space to save the object/file.
            // A certain amount of space is reserved for each file type.
            auto estimated_size = filesize ? *filesize : getEstimatedSizeOfFileType(file_type);
            if (!reserveSpaceImpl(file_type, estimated_size, EvictMode::TryEvict, lock))
            {
                // Space still not enough after eviction.
                GET_METRIC(tiflash_storage_remote_cache, type_dtfile_full).Increment();
                LOG_DEBUG(
                    log,
                    "s3_key={} space not enough(capacity={} used={} estimated_size={}), skip cache",
                    s3_key,
                    cache_capacity,
                    cache_used,
                    estimated_size);
                return nullptr;
            }

            file_seg = std::make_shared<FileSegment>(
                toLocalFilename(s3_key),
                FileSegment::Status::Empty,
                estimated_size,
                file_type);
            table.set(s3_key, file_seg);
        }
    } // Release the lock before submitting bg download task. Because bgDownload may be blocked when the queue is full.

    if (wait_ms != 0)
    {
        // Follower path: another thread already inserted the `Empty` segment and is downloading this key.
        // Wait only for the configured bounded budget, then either reuse the completed file or return miss
        // so the caller can fall back without opening a duplicate download stream for the same object.
        Stopwatch wait_watch;
        auto status = file_seg->waitForNotEmptyFor(std::chrono::milliseconds(wait_ms));
        const auto waited_bytes = filesize.value_or(file_seg->getSize());
        if (status == FileSegment::Status::Complete)
        {
            observeWaitOnDownloadingMetrics(file_type, WaitResult::Hit, waited_bytes, wait_watch.elapsedSeconds());
            GET_METRIC(tiflash_storage_remote_cache, type_dtfile_hit).Increment();
            return file_seg;
        }

        observeWaitOnDownloadingMetrics(
            file_type,
            status == FileSegment::Status::Failed ? WaitResult::Failed : WaitResult::Timeout,
            waited_bytes,
            wait_watch.elapsedSeconds());
        // Timeout is intentionally surfaced as a cache miss here. The caller can fall back to another read path,
        // while the original downloader keeps making progress in background instead of being duplicated by followers.
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_miss).Increment();
        return nullptr;
    }

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
    if (!reserveSpaceImpl(file_type, estimated_size, EvictMode::ForceEvict, lock))
    {
        // Space still not enough after eviction.
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_full).Increment();
        LOG_INFO(
            log,
            "s3_key={} space not enough(capacity={} used={} estimated_size={}), skip cache",
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
    // Except current thread and the FileTable,
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

// Try best to reserve space for new coming file.
// return true if reservation success.
// `size` is the amount of space to reserve (cache_used will increase by this amount on success).
bool FileCache::reserveSpaceImpl(
    FileType reserve_for,
    UInt64 size,
    EvictMode mode,
    std::unique_lock<std::mutex> & guard)
{
    if (cache_used + size <= cache_capacity)
    {
        // If cache_capacity is enough, just reserve it.
        // reserve means that `cache_used` increase `size`.
        cache_used += size;
        CurrentMetrics::set(CurrentMetrics::DTFileCacheUsed, cache_used);
        return true;
    }

    evictBySizeImpl(reserve_for, size, cache_min_age_seconds.load(std::memory_order_relaxed), mode, guard);
    // try update `cache_used` after eviction
    if (cache_used + size <= cache_capacity)
    {
        // cache_capacity is enough after eviction. Mark reservation success.
        cache_used += size;
        CurrentMetrics::set(CurrentMetrics::DTFileCacheUsed, cache_used);
        return true;
    }
    return false;
}

// Evict files to free space for new coming file.
// `size_to_reserve` is the required space to reserve.
// `min_age_seconds` is the minimum age of files to be evicted.
// Return the total evicted size.
// Caller should ensure that `size_to_reserve` is larger than available space.
UInt64 FileCache::evictBySizeImpl(
    FileType evict_for,
    UInt64 size_to_reserve,
    UInt64 min_age_seconds,
    EvictMode mode,
    std::unique_lock<std::mutex> & guard)
{
    UInt64 min_evict_size = 0;
    if (cache_capacity < cache_used)
    {
        min_evict_size = cache_used - cache_capacity + size_to_reserve;
        LOG_WARNING(
            log,
            "evictBySizeImpl cache overused, capacity={} used={} reserve_size={} min_evict_size={} evict_mode={}",
            cache_capacity,
            cache_used,
            size_to_reserve,
            min_evict_size,
            magic_enum::enum_name(mode));
    }
    else
    {
        assert(cache_capacity >= cache_used); // not underflow
        assert(size_to_reserve > cache_capacity - cache_used); // not underflow, ensure by the caller
        min_evict_size = size_to_reserve - (cache_capacity - cache_used);
    }

    switch (mode)
    {
    case EvictMode::NoEvict:
        return 0;
    case EvictMode::ForceEvict:
        [[fallthrough]]; // share initial try-eviction logic with "TryEvict" mode; ForceEvict may do additional eviction later
    case EvictMode::TryEvict:
    {
        LOG_DEBUG(
            log,
            "tryEvictFile for {} min_evict_size={} evict_mode={}",
            magic_enum::enum_name(evict_for),
            min_evict_size,
            magic_enum::enum_name(mode));
        const UInt64 size_evicted_during_try = tryEvictFile(evict_for, min_evict_size, min_age_seconds, mode, guard);
        if (likely(min_evict_size <= size_evicted_during_try))
        {
            // has enough space after eviction, break
            return size_evicted_during_try;
        }

        assert(min_evict_size > size_evicted_during_try);
        auto min_evict_size_after_try = min_evict_size - size_evicted_during_try;
        if (mode == EvictMode::ForceEvict)
        {
            // After tryEvictFile, the space is still not sufficient,
            // so we do a force eviction.
            auto size_force_evicted = forceEvict(min_evict_size_after_try, guard);
            LOG_INFO(
                log,
                "forceEvict min_evict_size={} min_evict_size_after_try={} force_evicted_size={}",
                min_evict_size,
                min_evict_size_after_try,
                size_force_evicted);
            return size_evicted_during_try + size_force_evicted;
        }
        else
        {
            LOG_INFO(
                log,
                "tryEvictFile failed to evict enough space, "
                "min_evict_size={} min_evict_size_after_try={} evict_mode={}",
                min_evict_size,
                min_evict_size_after_try,
                magic_enum::enum_name(mode));
            return size_evicted_during_try;
        }
    }
    }
    __builtin_unreachable();
}

// The basic evict logic:
// Distinguish cache priority according to file type. The larger the file type, the lower the priority.
// If evict_same_type_first is true,
// First, try to evict files which not be used recently with the same type. => Try to evict old files.
// Second, try to evict files with lower priority. => Try to evict lower priority files.
// Finally, evict files with higher priority, if space is still not sufficient. Higher priority files
// are usually smaller. If we don't evict them, it is very possible that cache is full of these higher
// priority small files and we can't effectively cache any lower-priority large files.
std::vector<FileType> FileCache::getEvictFileTypes(FileType evict_for, bool evict_same_type_first)
{
    constexpr auto all_file_types = magic_enum::enum_values<FileType>(); // all_file_types are sorted by enum value.
    if (evict_same_type_first)
    {
        std::vector<FileType> evict_types;
        evict_types.push_back(evict_for); // First, try evict with the same file type.
        // Second, try evict from the lower priority file type.
        for (auto itr = std::rbegin(all_file_types); itr != std::rend(all_file_types); ++itr)
        {
            if (*itr != evict_for)
                evict_types.push_back(*itr);
        }
        return evict_types;
    }
    else
    {
        std::vector<FileType> evict_types;
        // Evict from the lower priority file type first.
        for (auto itr = std::rbegin(all_file_types); itr != std::rend(all_file_types); ++itr)
        {
            evict_types.push_back(*itr);
            if (*itr == evict_for)
            {
                // Do not evict higher priority file type
                break;
            }
        }
        return evict_types;
    }
}

// Try best to evict files to free space.
// `min_evict_size` is the required space to reserve.
// Return the total evicted size.
UInt64 FileCache::tryEvictFile(
    FileType evict_for,
    const UInt64 min_evict_size,
    const UInt64 min_age_seconds,
    EvictMode mode,
    std::unique_lock<std::mutex> & guard)
{
    RUNTIME_CHECK(mode != EvictMode::NoEvict);
    // shortcut
    if (min_evict_size == 0)
        return 0;

    UInt64 total_size_evicted = 0;
    auto file_types = getEvictFileTypes(evict_for, /*evict_same_type_first*/ true);
    for (auto evict_from : file_types)
    {
        // try to evict from `evict_from` file type.
        auto evicted_size = tryEvictFileFrom(evict_for, min_evict_size, min_age_seconds, evict_from, guard);
        total_size_evicted += evicted_size;
        if (total_size_evicted >= min_evict_size)
        {
            // has enough space after eviction, break
            break;
        }
    }
    return total_size_evicted;
}

UInt64 FileCache::tryEvictFileFrom(
    FileType evict_for,
    UInt64 min_evict_size,
    UInt64 min_age_seconds,
    FileType evict_from,
    std::unique_lock<std::mutex> & /*guard*/)
{
    auto & table = tables[static_cast<UInt64>(evict_from)];
    UInt64 total_released_size = 0;
    // max try evict times to prevent long time lock the FileCache
    constexpr UInt32 max_try_evict_count = 10;
    // File type that we evict for does not have higher priority,
    // so we need check last access time to prevent recently used files from evicting.
    bool check_last_access_time = evict_for >= evict_from;
    auto itr = table.begin();
    auto end = table.end();
    for (UInt32 try_evict_count = 0; try_evict_count < max_try_evict_count && itr != end; ++try_evict_count)
    {
        auto s3_key = *itr;
        // protected under guard
        auto f = table.get(s3_key, /*update_lru*/ false);
        if (!check_last_access_time || !f->isRecentlyAccess(std::chrono::seconds(min_age_seconds)))
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
        // has released enough space or tried enough times, break
        if (total_released_size >= min_evict_size)
        {
            break;
        }
    }

    LOG_DEBUG(
        log,
        "tryEvictFrom {} min_evict_size={} evicted_size={}",
        magic_enum::enum_name(evict_from),
        min_evict_size,
        total_released_size);
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

UInt64 FileCache::forceEvict(UInt64 size_to_reserve, std::unique_lock<std::mutex> & /*guard*/)
{
    if (unlikely(size_to_reserve == 0))
        return 0;

    // For a force evict, we simply evict from the oldest to the newest, until
    // space is sufficient.

    std::priority_queue<ForceEvictCandidate, std::vector<ForceEvictCandidate>, ForceEvictCandidateComparer>
        evict_candidates;

    // First, pick an item from all levels.
    // Note that access to `tables` is protected under `guard`
    size_t total_released_size = 0;

    constexpr auto all_file_types = magic_enum::enum_values<FileType>();
    std::vector<std::list<String>::iterator> each_type_lru_iters; // Stores the iterator of next candidate to add
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
        LOG_INFO(
            log,
            "ForceEvict {} size={} size_to_reserve={} total_released={}",
            to_evict.s3_key,
            released_size,
            size_to_reserve,
            total_released_size);
        if (released_size >= 0) // removed
        {
            total_released_size += released_size;
            if (total_released_size >= size_to_reserve)
                break;
        }
    }
    return total_released_size;
}

bool FileCache::reserveSpace(FileType reserve_for, UInt64 size, EvictMode mode)
{
    std::unique_lock lock(mtx);
    return reserveSpaceImpl(reserve_for, size, mode, lock);
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

FileCache::ShouldCacheRes FileCache::canCache(FileType file_type) const
{
    if (file_type == FileType::Unknown || static_cast<UInt64>(file_type) > cache_level)
    {
        return ShouldCacheRes::RejectTypeNotMatch;
    }
    auto max_bg_download_queue_size = logical_cores * max_downloading_count_scale.load(std::memory_order_relaxed);
    if (bg_downloading_count.load(std::memory_order_relaxed) >= max_bg_download_queue_size)
    {
        return ShouldCacheRes::RejectTooManyDownloading;
    }
    return ShouldCacheRes::Cache;
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
    case MutSup::extra_handle_id:
        return FileType::HandleColData;
    case MutSup::version_col_id:
        return FileType::VersionColData;
    case MutSup::delmark_col_id:
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
    else if (ext == ".vector")
    {
        return FileType::VectorIndex;
    }
    else if (ext == ".fulltext")
    {
        return FileType::FullTextIndex;
    }
    else if (ext == ".inverted")
    {
        return FileType::InvertedIndex;
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

    return FileType::Unknown;
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

void downloadToLocal(
    Aws::IOStream & istr,
    const String & fname,
    Int64 content_length,
    const WriteLimiterPtr & write_limiter,
    const std::shared_ptr<S3::S3ReadLimiter> & s3_read_limiter,
    const std::shared_ptr<S3::S3ReadMetricsRecorder> & s3_read_metrics_recorder)
{
    // create an empty file with write_limiter
    // each time `ofile.write` is called, the write speed will be controlled by the write_limiter.
    auto ofile = std::make_shared<PosixWritableFile>(fname, true, O_CREAT | O_WRONLY, 0666, write_limiter);
    // simply create an empty file
    if (unlikely(content_length <= 0))
        return;

    GET_METRIC(tiflash_storage_remote_cache_bytes, type_dtfile_download_bytes).Increment(content_length);
    constexpr Int64 max_buffer_size = 128 * 1024; // 128 KiB
    auto buffer_size = std::min<Int64>(content_length, max_buffer_size);
    if (s3_read_limiter == nullptr)
    {
        ReadBufferFromIStream rbuf(istr, buffer_size);
        WriteBufferFromWritableFile wbuf(ofile, buffer_size);
        copyData(rbuf, wbuf, content_length);
        if (s3_read_metrics_recorder != nullptr)
            s3_read_metrics_recorder->recordBytes(rbuf.count(), S3::S3ReadSource::FileCacheDownload);
        wbuf.sync();
        return;
    }

    // Keep each refill within the limiter-suggested chunk size. Otherwise a low byte limit would
    // turn every 128 KiB refill into an oversized borrowing request and let downloads run ahead
    // of the configured node-level budget.
    buffer_size = std::min<Int64>(
        buffer_size,
        static_cast<Int64>(s3_read_limiter->getSuggestedChunkSize(static_cast<UInt64>(buffer_size))));
    // The limiter-aware buffer preserves the old copyData/write-buffer path while charging the shared
    // S3 budget before each refill from the remote body stream.
    ReadBufferFromIStreamWithLimiter rbuf(istr, buffer_size, s3_read_limiter, S3::S3ReadSource::FileCacheDownload);
    WriteBufferFromWritableFile wbuf(ofile, buffer_size);
    copyData(rbuf, wbuf, content_length);
    if (s3_read_metrics_recorder != nullptr)
        s3_read_metrics_recorder->recordBytes(rbuf.count(), S3::S3ReadSource::FileCacheDownload);
    wbuf.sync();
}

void FileCache::downloadImpl(const String & s3_key, FileSegmentPtr & file_seg, const WriteLimiterPtr & write_limiter)
{
    Stopwatch sw;
    auto client = S3::ClientFactory::instance().sharedTiFlashClient();
    auto s3_read_limiter = client->getS3ReadLimiter();
    auto s3_read_metrics_recorder = client->getS3ReadMetricsRecorder();
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
    SYNC_FOR("before_FileCache::downloadImpl_reserve_size");
    if (!finalizeReservedSize(file_seg->getFileType(), file_seg->getSize(), content_length))
    {
        LOG_INFO(
            log,
            "Download finalizeReservedSize failed, s3_key={} seg_size={} size={}",
            s3_key,
            file_seg->getSize(),
            content_length);
        file_seg->setStatus(FileSegment::Status::Failed);
        return;
    }

    const auto & local_fname = file_seg->getLocalFileName();
    // download as a temp file then rename to a formal file
    prepareParentDir(local_fname);
    auto temp_fname = toTemporaryFilename(local_fname);
    SYNC_FOR("before_FileCache::downloadImpl_download_to_local");
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::file_cache_bg_download_fail);
    downloadToLocal(
        result.GetBody(),
        temp_fname,
        content_length,
        write_limiter,
        s3_read_limiter,
        s3_read_metrics_recorder);
    std::filesystem::rename(temp_fname, local_fname);

#ifndef NDEBUG
    // sanity check under debug mode
    auto fsize = std::filesystem::file_size(local_fname);
    RUNTIME_CHECK_MSG(
        fsize == static_cast<UInt64>(content_length),
        "local_fname={}, file_size={}, content_length={}",
        local_fname,
        fsize,
        content_length);
#endif

    capacity_metrics->addUsedSize(local_fname, content_length);
    // update the file segment size and set as complete
    file_seg->setComplete(content_length);
    LOG_INFO(
        log,
        "Download success, s3_key={} local={} size={} cost={}ms",
        s3_key,
        local_fname,
        content_length,
        sw.elapsedMilliseconds());
}

void FileCache::bgDownloadExecutor(
    const String & s3_key,
    FileSegmentPtr & file_seg,
    const WriteLimiterPtr & write_limiter,
    std::chrono::steady_clock::time_point enqueue_time)
{
    observeBgDownloadStageMetrics(
        file_seg->getFileType(),
        BgDownloadStage::QueueWait,
        std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - enqueue_time)
            .count());
    Stopwatch download_watch;
    try
    {
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_download).Increment();
        downloadImpl(s3_key, file_seg, write_limiter);
    }
    catch (...)
    {
        // ignore the exception here, and log as warning.
        tryLogCurrentWarningException(log, fmt::format("Download s3_key={} failed", s3_key));
    }
    observeBgDownloadStageMetrics(file_seg->getFileType(), BgDownloadStage::Download, download_watch.elapsedSeconds());
    if (!file_seg->isReadyToRead())
    {
        file_seg->setStatus(FileSegment::Status::Failed);
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_download_failed).Increment();
        bg_download_fail_count.fetch_add(1, std::memory_order_relaxed);
        file_seg.reset();
        // Followers may still hold the failed segment while waking up from bounded wait. Force removal so
        // the failed placeholder does not stay published in the cache table and block later retries.
        remove(s3_key, /*force*/ true);
    }
    else
    {
        bg_download_succ_count.fetch_add(1, std::memory_order_relaxed);
    }
    bg_downloading_count.fetch_sub(1, std::memory_order_relaxed);
    updateBgDownloadStatusMetrics(bg_downloading_count.load(std::memory_order_relaxed));
    LOG_DEBUG(
        log,
        "downloading count {} => s3_key {} finished",
        bg_downloading_count.load(std::memory_order_relaxed),
        s3_key);
}

void FileCache::bgDownload(const String & s3_key, FileSegmentPtr & file_seg)
{
    bg_downloading_count.fetch_add(1, std::memory_order_relaxed);
    updateBgDownloadStatusMetrics(bg_downloading_count.load(std::memory_order_relaxed));
    LOG_DEBUG(
        log,
        "downloading count {} => s3_key {} start",
        bg_downloading_count.load(std::memory_order_relaxed),
        s3_key);
    auto write_limiter = rate_limiter.getBgWriteLimiter();
    auto enqueue_time = std::chrono::steady_clock::now();
    S3FileCachePool::get().scheduleOrThrowOnError(
        [this, s3_key = s3_key, file_seg = file_seg, limiter = std::move(write_limiter), enqueue_time]() mutable {
            bgDownloadExecutor(s3_key, file_seg, limiter, enqueue_time);
        });
}

void FileCache::fgDownload(const String & s3_key, FileSegmentPtr & file_seg)
{
    SYNC_FOR("FileCache::fgDownload"); // simulate long s3 download

    try
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::file_cache_fg_download_fail);
        GET_METRIC(tiflash_storage_remote_cache, type_dtfile_download).Increment();
        // not limit write speed for foreground download now
        downloadImpl(s3_key, file_seg, nullptr);
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
        remove(s3_key, /*force*/ true);
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
            if (canCache(file_type) == FileCache::ShouldCacheRes::Cache && cache_capacity - cache_used >= size)
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
    bool has_changed = false;

    double new_download_scale = settings.dt_filecache_downloading_count_scale;
    auto old_download_scale = download_count_scale.load(std::memory_order_relaxed);
    size_t new_concurrency = logical_cores * old_download_scale;
    if (std::fabs(new_download_scale - old_download_scale) > 0.001)
    {
        if (likely(new_download_scale > 0.0))
        {
            download_count_scale.store(new_download_scale, std::memory_order_relaxed);
            new_concurrency = logical_cores * new_download_scale;
            has_changed = true;
        }
        else
        {
            LOG_WARNING(
                log,
                "dt_filecache_downloading_count_scale is set to non-positive value, ignore it, value={}",
                new_download_scale);
        }
    }
    new_concurrency = std::max(new_concurrency, 1); // at least 1

    double new_queue_scale = settings.dt_filecache_max_downloading_count_scale;
    auto old_queue_scale = max_downloading_count_scale.load(std::memory_order_relaxed);
    size_t new_queue_size = logical_cores * old_queue_scale;
    if (std::fabs(new_queue_scale - old_queue_scale) > 0.001)
    {
        if (likely(new_queue_scale > 0.0))
        {
            max_downloading_count_scale.store(new_queue_scale, std::memory_order_relaxed);
            new_queue_size = logical_cores * new_queue_scale;
            has_changed = true;
        }
        else if (std::fabs(new_queue_scale) < 0.0001)
        {
            max_downloading_count_scale.store(0.0, std::memory_order_relaxed);
            has_changed = true;
            LOG_WARNING(
                log,
                "dt_filecache_max_downloading_count_scale is set to zero, disable download file from S3, value={}",
                new_queue_scale);
        }
        else
        {
            LOG_WARNING(
                log,
                "dt_filecache_max_downloading_count_scale is set to non-positive value, ignore it, value={}",
                new_queue_scale);
        }
    }
    new_queue_size = std::max(new_queue_size, new_concurrency); // at least the same as concurrency

    if (has_changed)
    {
        LOG_INFO(
            log,
            "Update S3FileCachePool config: "
            "logical_cores={} "
            "old_download_scale={} download_scale={} download_concurrency={} "
            "old_queue_scale={} queue_scale={} queue_size={}",
            logical_cores,
            old_download_scale,
            download_count_scale.load(std::memory_order_relaxed),
            new_concurrency,
            old_queue_scale,
            max_downloading_count_scale.load(std::memory_order_relaxed),
            new_queue_size);
        S3FileCachePool::get().setQueueSize(new_queue_size);
        S3FileCachePool::get().setMaxThreads(new_concurrency);
    }

    UInt64 cache_min_age = settings.dt_filecache_min_age_seconds;
    if (cache_min_age != cache_min_age_seconds.load(std::memory_order_relaxed))
    {
        LOG_INFO(
            log,
            "Update S3FileCachePool config: cache_min_age_seconds {} => {}",
            cache_min_age_seconds.load(std::memory_order_relaxed),
            cache_min_age);
        cache_min_age_seconds.store(cache_min_age, std::memory_order_relaxed);
    }

    UInt64 new_wait_ms = settings.dt_filecache_wait_on_downloading_ms;
    if (new_wait_ms != wait_on_downloading_ms.load(std::memory_order_relaxed))
    {
        LOG_INFO(
            log,
            "Update S3FileCache bounded wait config: wait_on_downloading_ms {} => {}",
            wait_on_downloading_ms.load(std::memory_order_relaxed),
            new_wait_ms);
        wait_on_downloading_ms.store(new_wait_ms, std::memory_order_relaxed);
    }
}

// Evict the cached files until no file of >= `file_type` is in cache.
UInt64 FileCache::evictByFileType(FileSegment::FileType file_type)
{
    // getEvictFileTypes is a static method that is not related to the current object state,
    // so it is safe to call it before acquiring the lock.
    auto file_types = getEvictFileTypes(file_type, /*evict_same_type_first*/ false);
    std::lock_guard lock(mtx);
    UInt64 total_released_size = 0;
    for (auto evict_from : file_types)
    {
        UInt64 curr_released_size = 0;
        auto & table = tables[static_cast<UInt64>(evict_from)];
        for (auto itr = table.begin(); itr != table.end();)
        {
            auto s3_key = *itr;
            auto f = table.get(s3_key, /*update_lru*/ false);
            auto [released_size, next_itr] = removeImpl(table, s3_key, f, /*force*/ true);
            if (released_size < 0) // not remove
            {
                ++itr;
            }
            else // removed
            {
                itr = next_itr;
                curr_released_size += released_size;
            }
        }
        total_released_size += curr_released_size;
        LOG_INFO(
            log,
            "evictByFileType layer evict finish, evict_from={} file_type={} released_size={} tot_release_size={}",
            magic_enum::enum_name(evict_from),
            magic_enum::enum_name(file_type),
            curr_released_size,
            total_released_size);
    }
    LOG_INFO(
        log,
        "evictByFileType finish, file_type={} total_released_size={}",
        magic_enum::enum_name(file_type),
        total_released_size);
    return total_released_size;
}

// Evict the cached files until at least `size_to_reserve` bytes are freed.
// Return the actual evicted size.
// When `min_age_seconds == 0`, this function uses the current `cache_min_age_seconds`; otherwise it uses the given value.
// If `force_evict` is true, it will evict files even if they are being used recently.
UInt64 FileCache::evictBySize(UInt64 size_to_reserve, UInt64 min_age_seconds, bool force_evict)
{
    std::unique_lock lock(mtx);
    if (size_to_reserve + cache_used <= cache_capacity)
    {
        // shortcut for no need evict
        LOG_INFO(
            log,
            "evictBySize no need evict, size_to_reserve={} force_evict={} cache_capacity={} cache_used={}",
            size_to_reserve,
            force_evict,
            cache_capacity,
            cache_used);
        return 0;
    }

    UInt64 min_age = min_age_seconds == 0 ? cache_min_age_seconds.load(std::memory_order_relaxed) : min_age_seconds;
    EvictMode mode = force_evict ? EvictMode::ForceEvict : EvictMode::TryEvict;
    // Should always use the last file type(lowest priority) to evict,
    // in order to respect the priority of file types and avoid evicting high priority files
    // by last_access_time in non-force evict.
    constexpr FileType max_file_type = magic_enum::enum_values<FileType>()[magic_enum::enum_count<FileType>() - 1];
    auto total_released_size = evictBySizeImpl(max_file_type, size_to_reserve, min_age, mode, lock);
    LOG_INFO(
        log,
        "evictBySize finish, size_to_reserve={} min_age={} force_evict={} total_released_size={} cache_capacity={} "
        "cache_used={}",
        size_to_reserve,
        min_age,
        force_evict,
        total_released_size,
        cache_capacity,
        cache_used);
    return total_released_size;
}

std::vector<std::tuple<FileSegment::FileType, CacheSizeHistogram>> FileCache::getCacheSizeHistogram() const
{
    std::lock_guard lock(mtx);
    std::vector<std::tuple<FileSegment::FileType, CacheSizeHistogram>> result;
    for (const auto & file_type : magic_enum::enum_values<FileSegment::FileType>())
    {
        const auto & table = tables[static_cast<UInt64>(file_type)];
        if (table.size() == 0)
            continue;
        CacheSizeHistogram histogram = table.getCacheSizeHistogram();
        result.emplace_back(file_type, histogram);
    }
    return result;
}
} // namespace DB
