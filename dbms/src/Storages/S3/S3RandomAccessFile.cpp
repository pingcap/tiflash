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

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <IO/BaseFile/MemoryRandomAccessFile.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <optional>

namespace ProfileEvents
{
extern const Event S3GetObject;
extern const Event S3ReadBytes;
extern const Event S3GetObjectRetry;
extern const Event S3IORead;
extern const Event S3IOSeek;
extern const Event S3IOSeekBackward;
} // namespace ProfileEvents

namespace DB::S3
{
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
    , log(Logger::get(remote_fname))
    , scan_context(scan_context_)
{
    RUNTIME_CHECK(client_ptr != nullptr);
    RUNTIME_CHECK_MSG(initialize(), "Open S3 file for read fail, key={}", remote_fname);
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
} // namespace

ssize_t S3RandomAccessFile::read(char * buf, size_t size)
{
    while (true)
    {
        auto n = readImpl(buf, size);
        if (unlikely(n < 0 && isRetryableError(n, errno)))
        {
            // If it is a retryable error, then initialize again
            if (initialize())
            {
                continue;
            }
        }
        return n;
    }
}

ssize_t S3RandomAccessFile::readImpl(char * buf, size_t size)
{
    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IORead, 1);
    auto & istr = read_result.GetBody();
    istr.read(buf, size);
    size_t gcount = istr.gcount();
    // Theoretically, `istr.eof()` is equivalent to `cur_offset + gcount != static_cast<size_t>(content_length)`.
    // It's just a double check for more safety.
    if (gcount < size && (!istr.eof() || cur_offset + gcount != static_cast<size_t>(content_length)))
    {
        auto state = istr.rdstate();
        LOG_WARNING(
            log,
            "Cannot read from istream, size={} gcount={} state=0x{:02X} cur_offset={} content_length={} errno={} "
            "errmsg={} cost={}ns",
            size,
            gcount,
            state,
            cur_offset,
            content_length,
            errno,
            strerror(errno),
            sw.elapsed());
        return (state & std::ios_base::failbit || state & std::ios_base::badbit) ? S3StreamError : S3UnknownError;
    }
    auto elapsed_secs = sw.elapsedSeconds();
    if (scan_context)
    {
        scan_context->disagg_s3file_read_time_ms += elapsed_secs * 1000;
        scan_context->disagg_s3file_read_count += 1;
        scan_context->disagg_s3file_read_bytes += gcount;
    }
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_secs);
    if (elapsed_secs > 0.01) // 10ms
    {
        LOG_DEBUG(
            log,
            "gcount={} cur_offset={} content_length={} cost={:.3f}s",
            gcount,
            cur_offset,
            content_length,
            elapsed_secs);
    }
    cur_offset += gcount;
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, gcount);
    return gcount;
}

off_t S3RandomAccessFile::seek(off_t offset_, int whence)
{
    while (true)
    {
        auto off = seekImpl(offset_, whence);
        if (unlikely(off < 0 && isRetryableError(off, errno)))
        {
            // If it is a retryable error, then initialize again
            if (initialize())
            {
                continue;
            }
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
        cur_offset = offset_;
        cur_retry = 0;

        if (!initialize())
        {
            return S3StreamError;
        }
        return cur_offset;
    }

    // Forward seek
    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IOSeek, 1);
    auto & istr = read_result.GetBody();
    if (!istr.ignore(offset_ - cur_offset))
    {
        auto state = istr.rdstate();
        LOG_WARNING(
            log,
            "Cannot ignore from istream, state=0x{:02X}, errno={} errmsg={} cost={}ns",
            state,
            errno,
            strerror(errno),
            sw.elapsed());
        return (state & std::ios_base::failbit || state & std::ios_base::badbit) ? S3StreamError : S3UnknownError;
    }
    auto elapsed_secs = sw.elapsedSeconds();
    if (scan_context)
    {
        scan_context->disagg_s3file_seek_time_ms += elapsed_secs * 1000;
        scan_context->disagg_s3file_seek_count += 1;
        scan_context->disagg_s3file_seek_bytes += offset_ - cur_offset;
    }
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_secs);
    if (elapsed_secs > 0.01) // 10ms
    {
        LOG_DEBUG(
            log,
            "ignore_count={} cur_offset={} content_length={} cost={:.3f}s",
            offset_ - cur_offset,
            cur_offset,
            content_length,
            elapsed_secs);
    }
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, offset_ - cur_offset);
    cur_offset = offset_;
    return cur_offset;
}
String S3RandomAccessFile::readRangeOfObject()
{
    return fmt::format("bytes={}-", cur_offset);
}

bool S3RandomAccessFile::initialize()
{
    bool request_succ = false;
    Aws::S3::Model::GetObjectRequest req;
    req.SetRange(readRangeOfObject());
    client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
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
        cur_retry += 1;
        ProfileEvents::increment(ProfileEvents::S3GetObject);
        if (cur_retry > 1)
        {
            ProfileEvents::increment(ProfileEvents::S3GetObjectRetry);
        }
        auto outcome = client_ptr->GetObject(req);
        if (!outcome.IsSuccess())
        {
            auto el = sw_get_object.elapsedSeconds();
            LOG_WARNING(
                log,
                "S3 GetObject failed: {}, retry={}/{}, key={}, elapsed{}={:.3f}s",
                S3::S3ErrorMessage(outcome.GetError()),
                cur_retry,
                max_retry,
                req.GetKey(),
                el > 60.0 ? "(long)" : "",
                el);
            continue;
        }

        request_succ = true;
        if (content_length == 0)
        {
            content_length = outcome.GetResult().GetContentLength();
        }
        read_result = outcome.GetResultWithOwnership();
        RUNTIME_CHECK(read_result.GetBody(), remote_fname, strerror(errno));
        break;
    }
    return request_succ;
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
