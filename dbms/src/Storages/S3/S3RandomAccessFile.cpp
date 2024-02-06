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
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashMetrics.h>
#include <IO/BaseFile/MemoryRandomAccessFile.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <common/likely.h>

#include <optional>

namespace ProfileEvents
{
extern const Event S3GetObject;
extern const Event S3ReadBytes;
extern const Event S3GetObjectRetry;
} // namespace ProfileEvents

namespace DB::S3
{
S3RandomAccessFile::S3RandomAccessFile(std::shared_ptr<TiFlashS3Client> client_ptr_, const String & remote_fname_)
    : client_ptr(std::move(client_ptr_))
    , remote_fname(remote_fname_)
    , cur_offset(0)
    , log(Logger::get(remote_fname))
{
    RUNTIME_CHECK(client_ptr != nullptr);
    RUNTIME_CHECK(initialize(), remote_fname);
}

std::string S3RandomAccessFile::getFileName() const
{
    return fmt::format("{}/{}", client_ptr->bucket(), remote_fname);
}

bool isRetryableError(int e)
{
    return e == ECONNRESET || e == EAGAIN;
}

ssize_t S3RandomAccessFile::read(char * buf, size_t size)
{
    while (true)
    {
        auto n = readImpl(buf, size);
        if (unlikely(n < 0 && isRetryableError(errno)))
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
    auto & istr = read_result.GetBody();
    istr.read(buf, size);
    size_t gcount = istr.gcount();
    // Theoretically, `istr.eof()` is equivalent to `cur_offset + gcount != static_cast<size_t>(content_length)`.
    // It's just a double check for more safty.
    if (gcount < size && (!istr.eof() || cur_offset + gcount != static_cast<size_t>(content_length)))
    {
        LOG_ERROR(
            log,
            "Cannot read from istream, size={} gcount={} state=0x{:02X} cur_offset={} content_length={} errmsg={} "
            "cost={}ns",
            size,
            gcount,
            istr.rdstate(),
            cur_offset,
            content_length,
            strerror(errno),
            sw.elapsed());
        return -1;
    }
    auto elapsed_ns = sw.elapsed();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_ns / 1000000000.0);
    if (elapsed_ns > 10000000) // 10ms
    {
        LOG_DEBUG(
            log,
            "gcount={} cur_offset={} content_length={} cost={}ns",
            gcount,
            cur_offset,
            content_length,
            elapsed_ns);
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
        if (unlikely(off < 0 && isRetryableError(errno)))
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
        offset_ >= cur_offset && offset_ <= content_length,
        "Seek position is out of bounds: offset={}, cur_offset={}, content_length={}",
        offset_,
        cur_offset,
        content_length);

    if (offset_ == cur_offset)
    {
        return cur_offset;
    }
    Stopwatch sw;
    auto & istr = read_result.GetBody();
    if (!istr.ignore(offset_ - cur_offset))
    {
        LOG_ERROR(log, "Cannot ignore from istream, errmsg={}, cost={}ns", strerror(errno), sw.elapsed());
        return -1;
    }
    auto elapsed_ns = sw.elapsed();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_ns / 1000000000.0);
    if (elapsed_ns > 10000000) // 10ms
    {
        LOG_DEBUG(
            log,
            "ignore_count={} cur_offset={} content_length={} cost={}ns",
            offset_ - cur_offset,
            cur_offset,
            content_length,
            elapsed_ns);
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
    Stopwatch sw;
    bool request_succ = false;
    Aws::S3::Model::GetObjectRequest req;
    req.SetRange(readRangeOfObject());
    client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
    while (cur_retry < max_retry)
    {
        cur_retry += 1;
        ProfileEvents::increment(ProfileEvents::S3GetObject);
        if (cur_retry > 1)
        {
            ProfileEvents::increment(ProfileEvents::S3GetObjectRetry);
        }
        auto outcome = client_ptr->GetObject(req);
        if (!outcome.IsSuccess())
        {
            LOG_ERROR(log, "S3 GetObject failed: {}, cur_retry={}", S3::S3ErrorMessage(outcome.GetError()), cur_retry);
            continue;
        }

        request_succ = true;
        if (content_length == 0)
        {
            content_length = outcome.GetResult().GetContentLength();
        }
        read_result = outcome.GetResultWithOwnership();
        RUNTIME_CHECK(read_result.GetBody(), remote_fname, strerror(errno));
        GET_METRIC(tiflash_storage_s3_request_seconds, type_get_object).Observe(sw.elapsedSeconds());
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
            scan_context.value()->disagg_read_cache_hit_size += filesize.value();
        return file;
    }
    if (scan_context.has_value())
        scan_context.value()->disagg_read_cache_miss_size += filesize.value();
    auto & ins = S3::ClientFactory::instance();
    return std::make_shared<S3RandomAccessFile>(ins.sharedTiFlashClient(), remote_fname);
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
