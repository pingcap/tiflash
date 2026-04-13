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
#include <Common/TiFlashMetrics.h>
#include <IO/BaseFile/MemoryRandomAccessFile.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/S3/S3ReadLimiter.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <fiu.h>

#include <optional>
#include <random>
#include <string_view>

namespace CurrentMetrics
{
extern const Metric S3RandomAccessFile;
}
namespace ProfileEvents
{
extern const Event S3GetObject;
extern const Event S3ReadBytes;
extern const Event S3GetObjectRetry;
extern const Event S3IORead;
extern const Event S3IOReadError;
extern const Event S3IOSeek;
extern const Event S3IOSeekError;
extern const Event S3IOSeekBackward;
} // namespace ProfileEvents
namespace DB::FailPoints
{
extern const char force_s3_random_access_file_init_fail[];
extern const char force_s3_random_access_file_read_fail[];
extern const char force_s3_random_access_file_seek_fail[];
extern const char force_s3_random_access_file_seek_chunked[];
} // namespace DB::FailPoints

namespace DB::S3
{
namespace
{
constexpr size_t s3_read_limiter_preferred_chunk_size = 128 * 1024;
constexpr size_t s3_forward_seek_reopen_threshold = 128 * 1024;
}

String S3RandomAccessFile::summary() const
{
    return fmt::format("remote_fname={} cur_offset={} cur_retry={}", remote_fname, cur_offset, cur_retry);
}

S3RandomAccessFile::S3RandomAccessFile(
    std::shared_ptr<TiFlashS3Client> client_ptr_,
    const String & remote_fname_,
    const DM::ScanContextPtr & scan_context_)
    : client_ptr(std::move(client_ptr_))
    , remote_fname(remote_fname_)
    , cur_offset(0)
    , read_limiter(nullptr)
    , read_metrics_recorder(nullptr)
    , log(Logger::get(remote_fname))
    , scan_context(scan_context_)
{
    RUNTIME_CHECK(client_ptr != nullptr);
    read_limiter = client_ptr->getS3ReadLimiter();
    read_metrics_recorder = client_ptr->getS3ReadMetricsRecorder();
    initialize("init file");
    CurrentMetrics::add(CurrentMetrics::S3RandomAccessFile);
}

S3RandomAccessFile::~S3RandomAccessFile()
{
    CurrentMetrics::sub(CurrentMetrics::S3RandomAccessFile);
}

std::string S3RandomAccessFile::getFileName() const
{
    return fmt::format("{}/{}", client_ptr->bucket(), remote_fname);
}

std::string S3RandomAccessFile::getInitialFileName() const
{
    return remote_fname;
}

namespace
{
constexpr int S3UnknownError = -1;
constexpr int S3StreamError = -2;

bool isRetryableError(int ret, int err)
{
    return ret == S3StreamError || err == ECONNRESET || err == EAGAIN || err == EINPROGRESS;
}

bool shouldRetryStreamError(Int32 retried_times, int ret, int err, Int32 max_retry_times)
{
    return retried_times + 1 < max_retry_times && isRetryableError(ret, err);
}
} // namespace

ssize_t S3RandomAccessFile::read(char * buf, size_t size)
{
    for (Int32 stream_retry_times = 0;; ++stream_retry_times)
    {
        auto n = readImpl(buf, size);
        if (unlikely(n < 0 && shouldRetryStreamError(stream_retry_times, n, errno, max_retry)))
        {
            // Stream-side retries reopen from the last committed offset instead of sharing initialize state.
            reopenAt(cur_offset, "read meet retryable error");
            continue;
        }
        return n;
    }
}

ssize_t S3RandomAccessFile::readImpl(char * buf, size_t size)
{
    if (read_limiter != nullptr && read_limiter->maxReadBytesPerSec() > 0)
        // Charge the shared node-level budget in small chunks instead of allowing a single large `read()` to burst.
        return readChunked(buf, size);

    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IORead, 1);
    auto & istr = read_result.GetBody();
    istr.read(buf, size);
    return finalizeRead(size, istr.gcount(), sw, istr);
}

ssize_t S3RandomAccessFile::readChunked(char * buf, size_t size)
{
    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IORead, 1);

    auto & istr = read_result.GetBody();
    // Use the limiter-suggested step so one large logical read is split into smoother refill-period-
    // sized chunks. That keeps `requestBytes()` on its strict path for normal reads and only falls
    // back to borrowing semantics for requests that are unavoidably larger than one burst.
    const auto chunk_size = read_limiter->getSuggestedChunkSize(s3_read_limiter_preferred_chunk_size);
    size_t total_gcount = 0;
    while (total_gcount < size)
    {
        // The limiter charges requested bytes before the actual stream read so direct reads and FileCache downloads
        // compete for the same node-level remote-read budget. This is intentionally conservative: a short read still
        // spends the full requested budget for this chunk. If we need tighter accounting later, we can add a
        // compensation path based on the actual bytes read back from S3.
        auto to_read = std::min(size - total_gcount, static_cast<size_t>(chunk_size));
        read_limiter->requestBytes(to_read, S3ReadSource::DirectRead);
        istr.read(buf + total_gcount, to_read);
        auto gcount = istr.gcount();
        total_gcount += gcount;
        if (static_cast<size_t>(gcount) < to_read)
            break;
    }

    return finalizeRead(size, total_gcount, sw, istr);
}

ssize_t S3RandomAccessFile::finalizeRead(
    size_t requested_size,
    size_t actual_size,
    const Stopwatch & sw,
    std::istream & istr)
{
    // Keep the post-read handling shared so limiter and non-limiter paths emit identical retries, logging and
    // observability signals.
    fiu_do_on(FailPoints::force_s3_random_access_file_read_fail, {
        LOG_WARNING(log, "failpoint force_s3_random_access_file_read_fail is triggered, return S3StreamError");
        return S3StreamError;
    });

    // Theoretically, `istr.eof()` is equivalent to `cur_offset + actual_size != static_cast<size_t>(content_length)`.
    // It's just a double check for more safety.
    if (actual_size < requested_size
        && (!istr.eof() || cur_offset + actual_size != static_cast<size_t>(content_length)))
    {
        ProfileEvents::increment(ProfileEvents::S3IOReadError);
        auto state = istr.rdstate();
        auto elapsed_secs = sw.elapsedSeconds();
        GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream_err).Observe(elapsed_secs);
        LOG_WARNING(
            log,
            "Cannot read from istream, size={} gcount={} state=0x{:02X} cur_offset={} content_length={} "
            "errno={} errmsg={} cost={:.6f}s",
            requested_size,
            actual_size,
            state,
            cur_offset,
            content_length,
            errno,
            strerror(errno),
            elapsed_secs);
        return (state & std::ios_base::failbit || state & std::ios_base::badbit) ? S3StreamError : S3UnknownError;
    }

    auto elapsed_secs = sw.elapsedSeconds();
    if (scan_context)
    {
        scan_context->disagg_s3file_read_time_ms += elapsed_secs * 1000;
        scan_context->disagg_s3file_read_count += 1;
        scan_context->disagg_s3file_read_bytes += actual_size;
    }
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_secs);
    if (elapsed_secs > 0.01)
    {
        LOG_DEBUG(
            log,
            "gcount={} cur_offset={} content_length={} cost={:.3f}s",
            actual_size,
            cur_offset,
            content_length,
            elapsed_secs);
    }
    cur_offset += actual_size;
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, actual_size);
    if (read_metrics_recorder != nullptr)
        read_metrics_recorder->recordBytes(actual_size, S3ReadSource::DirectRead);
    return actual_size;
}

off_t S3RandomAccessFile::seek(off_t offset_, int whence)
{
    for (Int32 stream_retry_times = 0;; ++stream_retry_times)
    {
        auto off = seekImpl(offset_, whence);
        if (unlikely(off < 0 && shouldRetryStreamError(stream_retry_times, off, errno, max_retry)))
        {
            // Retry the seek from the last committed offset rather than from a partially drained stream.
            reopenAt(cur_offset, "seek meet retryable error");
            continue;
        }
        return off;
    }
}

off_t S3RandomAccessFile::seekImpl(off_t offset_, int whence)
{
    RUNTIME_CHECK_MSG(whence == SEEK_SET, "Only SEEK_SET mode is allowed, but {} is received", whence);
    RUNTIME_CHECK_MSG(
        offset_ >= 0 && offset_ <= content_length,
        "Seek position is out of bounds: offset={}, cur_offset={}, content_length={}",
        offset_,
        cur_offset,
        content_length);

    if (offset_ == cur_offset)
    {
        return cur_offset;
    }

    if (offset_ < cur_offset)
    {
        ProfileEvents::increment(ProfileEvents::S3IOSeekBackward, 1);
        // The current body stream is forward-only. Re-open from the target offset.
        reopenAt(offset_, "seek backward");
        return cur_offset;
    }

    auto bytes_to_ignore = static_cast<size_t>(offset_ - cur_offset);
    if (shouldReopenForForwardSeek(bytes_to_ignore))
    {
        Stopwatch sw;
        ProfileEvents::increment(ProfileEvents::S3IOSeek, 1);
        // Large forward seeks are treated as logical repositioning. Reopen avoids draining the old body stream.
        reopenAt(offset_, "seek forward by reopen");
        return recordSuccessfulSeek(offset_, bytes_to_ignore, /* remote_read_bytes */ 0, sw);
    }

    if (read_limiter != nullptr && read_limiter->maxReadBytesPerSec() > 0)
        return seekChunked(offset_);

    // Forward seek
    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IOSeek, 1);
    auto & istr = read_result.GetBody();
    istr.ignore(bytes_to_ignore);
    return finalizeSeek(offset_, bytes_to_ignore, istr.gcount(), sw, istr);
}

off_t S3RandomAccessFile::seekChunked(off_t offset)
{
    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IOSeek, 1);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::force_s3_random_access_file_seek_chunked);
    auto & istr = read_result.GetBody();
    // Use the same chunk heuristic as readChunked() so forward seeks do not turn into one oversized
    // limiter request when skipping a large remote range.
    const auto chunk_size = read_limiter->getSuggestedChunkSize(s3_read_limiter_preferred_chunk_size);
    size_t total_ignored = 0;
    const auto bytes_to_ignore = static_cast<size_t>(offset - cur_offset);
    while (total_ignored < bytes_to_ignore)
    {
        // `ignore()` still drains the response body from S3, so it must be accounted against the same byte budget.
        auto to_ignore = std::min(bytes_to_ignore - total_ignored, static_cast<size_t>(chunk_size));
        read_limiter->requestBytes(to_ignore, S3ReadSource::DirectRead);
        istr.ignore(to_ignore);
        auto ignored = istr.gcount();
        total_ignored += ignored;
        if (static_cast<size_t>(ignored) < to_ignore)
            break;
    }

    return finalizeSeek(offset, bytes_to_ignore, total_ignored, sw, istr);
}

off_t S3RandomAccessFile::finalizeSeek(
    off_t target_offset,
    size_t requested_size,
    size_t actual_size,
    const Stopwatch & sw,
    std::istream & istr)
{
    // Keep post-seek handling shared so limiter and non-limiter paths emit identical retries, logging and
    // observability signals.
    fiu_do_on(FailPoints::force_s3_random_access_file_seek_fail, {
        LOG_WARNING(log, "failpoint force_s3_random_access_file_seek_fail is triggered, return S3StreamError");
        return S3StreamError;
    });

    if (actual_size < requested_size)
    {
        ProfileEvents::increment(ProfileEvents::S3IOSeekError);
        auto state = istr.rdstate();
        auto elapsed_secs = sw.elapsedSeconds();
        GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream_err).Observe(elapsed_secs);
        LOG_WARNING(
            log,
            "Cannot ignore from istream, state=0x{:02X}, ignored={} expected={} target_offset={} cur_offset={} "
            "content_length={} limiter_enabled={} max_read_bytes_per_sec={} errno={} errmsg={} cost={:.6f}s",
            state,
            actual_size,
            requested_size,
            target_offset,
            cur_offset,
            content_length,
            read_limiter != nullptr,
            read_limiter != nullptr ? read_limiter->maxReadBytesPerSec() : 0,
            errno,
            strerror(errno),
            elapsed_secs);
        return (state & std::ios_base::failbit || state & std::ios_base::badbit) ? S3StreamError : S3UnknownError;
    }

    return recordSuccessfulSeek(target_offset, requested_size, actual_size, sw);
}

void S3RandomAccessFile::reopenAt(off_t target_offset, std::string_view action)
{
    cur_offset = target_offset;
    // Each reopen starts a fresh initialize session. Stream-side retries must not inherit old GetObject debt.
    cur_retry = 0;
    initialize(action);
}

bool S3RandomAccessFile::shouldReopenForForwardSeek(size_t bytes_to_skip) const
{
    return bytes_to_skip > s3_forward_seek_reopen_threshold;
}

off_t S3RandomAccessFile::recordSuccessfulSeek(
    off_t target_offset,
    size_t logical_seek_size,
    size_t remote_read_bytes,
    const Stopwatch & sw)
{
    auto elapsed_secs = sw.elapsedSeconds();
    if (scan_context)
    {
        scan_context->disagg_s3file_seek_time_ms += elapsed_secs * 1000;
        scan_context->disagg_s3file_seek_count += 1;
        scan_context->disagg_s3file_seek_bytes += logical_seek_size;
    }
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_secs);
    if (elapsed_secs > 0.01)
    {
        LOG_DEBUG(
            log,
            "target_offset={} logical_seek_size={} remote_read_bytes={} cur_offset={} content_length={} cost={:.3f}s",
            target_offset,
            logical_seek_size,
            remote_read_bytes,
            cur_offset,
            content_length,
            elapsed_secs);
    }
    if (remote_read_bytes > 0)
    {
        ProfileEvents::increment(ProfileEvents::S3ReadBytes, remote_read_bytes);
        if (read_metrics_recorder != nullptr)
            read_metrics_recorder->recordBytes(remote_read_bytes, S3ReadSource::DirectRead);
    }
    cur_offset = target_offset;
    return cur_offset;
}

String S3RandomAccessFile::readRangeOfObject()
{
    return fmt::format("bytes={}-", cur_offset);
}

namespace details
{
Int64 calculateDelayForNextRetry(Int64 attempted_retries)
{
    static thread_local std::mt19937 rng;
    static thread_local std::uniform_int_distribution<Int32> dist;
    // Maximum left shift factor is capped by ceil(log2(max_delay)), to avoid wrap-around and overflow into negative values:
    return std::min(dist(rng) % 1000 * (1 << std::min(attempted_retries, 15L)), 20000);
}
} // namespace details

void S3RandomAccessFile::initialize(std::string_view action)
{
    // `cur_retry` is per-initialize state, so every new initialize action starts from a clean budget.
    cur_retry = 0;
    while (cur_retry < max_retry)
    {
        Stopwatch sw_get_object;
        SCOPE_EXIT({
            auto elapsed_secs = sw_get_object.elapsedSeconds();
            if (scan_context)
            {
                scan_context->disagg_s3file_get_object_ms += elapsed_secs * 1000;
                scan_context->disagg_s3file_get_object_count += 1;
            }
            GET_METRIC(tiflash_storage_s3_request_seconds, type_get_object).Observe(elapsed_secs);
        });
        ProfileEvents::increment(ProfileEvents::S3GetObject);
        if (cur_retry > 0)
        {
            ProfileEvents::increment(ProfileEvents::S3GetObjectRetry);
        }

        Aws::S3::Model::GetObjectRequest req;
        req.SetRange(readRangeOfObject());
        client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
        auto outcome = client_ptr->GetObject(req);
        fiu_do_on(FailPoints::force_s3_random_access_file_init_fail, {
            LOG_WARNING(log, "failpoint force_s3_random_access_file_init_fail is triggered, set outcome to error");
            outcome = Aws::S3::Model::GetObjectOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                Aws::S3::S3Errors::INTERNAL_FAILURE,
                "InternalError",
                "Injected error by failpoint",
                true));
        });
        if (!outcome.IsSuccess())
        {
            Int64 delay_ms = details::calculateDelayForNextRetry(cur_retry);
            cur_retry += 1;
            auto el = sw_get_object.elapsedSeconds();
            LOG_WARNING(
                log,
                "S3 GetObject failed: {}, retry={}/{}, key={}, elapsed{}={:.3f}s,"
                " now waiting {} ms before attempting again",
                S3::S3ErrorMessage(outcome.GetError()),
                cur_retry,
                max_retry,
                req.GetKey(),
                el > 60.0 ? "(long)" : "",
                el,
                delay_ms);
            // Sleep before next retry
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            continue;
        }

        if (content_length == 0)
        {
            content_length = outcome.GetResult().GetContentLength();
        }
        read_result = outcome.GetResultWithOwnership();
        RUNTIME_CHECK(read_result.GetBody(), remote_fname, strerror(errno));
        return; // init successfully
    }
    // exceed max retry times
    throw Exception(
        ErrorCodes::S3_ERROR,
        "Open S3 file for read fail after retries when {}, key={}",
        action,
        remote_fname);
}

inline static RandomAccessFilePtr tryOpenCachedFile(const String & remote_fname, std::optional<UInt64> filesize)
{
    try
    {
        auto * file_cache = FileCache::instance();
        return file_cache != nullptr
            ? file_cache->getRandomAccessFile(S3::S3FilenameView::fromKey(remote_fname), filesize)
            : nullptr;
    }
    catch (...)
    {
        tryLogCurrentException("tryOpenCachedFile", remote_fname);
        return nullptr;
    }
}

inline static RandomAccessFilePtr createFromNormalFile(
    const String & remote_fname,
    std::optional<UInt64> filesize,
    std::optional<DM::ScanContextPtr> scan_context)
{
    auto file = tryOpenCachedFile(remote_fname, filesize);
    if (file != nullptr)
    {
        if (scan_context.has_value())
        {
            scan_context.value()->disagg_read_cache_hit_size += filesize.value();
            scan_context.value()->disagg_s3file_hit_count++;
        }
        return file;
    }
    if (scan_context.has_value())
    {
        scan_context.value()->disagg_read_cache_miss_size += filesize.value();
        scan_context.value()->disagg_s3file_miss_count++;
    }
    auto & ins = S3::ClientFactory::instance();
    return std::make_shared<S3RandomAccessFile>(
        ins.sharedTiFlashClient(),
        remote_fname,
        scan_context ? *scan_context : nullptr);
}

RandomAccessFilePtr S3RandomAccessFile::create(const String & remote_fname)
{
    if (read_file_info)
        return createFromNormalFile(
            remote_fname,
            std::optional<UInt64>(read_file_info->size),
            read_file_info->scan_context != nullptr ? std::optional<DM::ScanContextPtr>(read_file_info->scan_context)
                                                    : std::nullopt);
    else
        return createFromNormalFile(remote_fname, std::nullopt, std::nullopt);
}
} // namespace DB::S3
