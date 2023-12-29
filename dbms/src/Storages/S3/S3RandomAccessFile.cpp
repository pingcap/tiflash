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
#include <Encryption/RandomAccessFile.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/MemoryRandomAccessFile.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <chrono>
#include <optional>
#include <thread>

namespace ProfileEvents
{
extern const Event S3GetObject;
extern const Event S3ReadBytes;
extern const Event S3GetObjectRetry;
} // namespace ProfileEvents

namespace DB::S3
{
S3RandomAccessFile::S3RandomAccessFile(
    std::shared_ptr<TiFlashS3Client> client_ptr_,
    const String & remote_fname_,
    std::optional<std::pair<UInt64, UInt64>> offset_and_size_in_object_)
    : client_ptr(std::move(client_ptr_))
    , remote_fname(remote_fname_)
    , offset_and_size_in_object(offset_and_size_in_object_)
    , cur_offset(0)
    , log(Logger::get(remote_fname))
{
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
            "Cannot read from istream, gcount={}, eof={}, cur_offset={}, content_length={}, errmsg={}, cost={}ns",
            gcount,
            istr.eof(),
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
    if (offset_and_size_in_object)
    {
        auto start = offset_and_size_in_object->first + cur_offset;
        auto end = offset_and_size_in_object->first + offset_and_size_in_object->second - 1;
        return fmt::format("bytes={}-{}", start, end);
    }
    else
    {
        return fmt::format("bytes={}-", cur_offset);
    }
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
        return file_cache != nullptr ? file_cache->getRandomAccessFile(S3::S3FilenameView::fromKey(remote_fname), filesize) : nullptr;
    }
    catch (...)
    {
        tryLogCurrentException("tryOpenCachedFile", remote_fname);
        return nullptr;
    }
}

inline static RandomAccessFilePtr createFromNormalFile(const String & remote_fname, std::optional<UInt64> filesize)
{
    auto file = tryOpenCachedFile(remote_fname, filesize);
    if (file != nullptr)
    {
        return file;
    }
    auto & ins = S3::ClientFactory::instance();
    return std::make_shared<S3RandomAccessFile>(ins.sharedTiFlashClient(), remote_fname);
}

inline static String readMergedSubFilesFromS3(const S3RandomAccessFile::ReadFileInfo & read_file_info_)
{
    auto & ins = S3::ClientFactory::instance();
    auto s3_key = S3::S3FilenameView::fromKeyWithPrefix(read_file_info_.merged_filename).toFullKey();
    auto s3_file = std::make_shared<S3RandomAccessFile>(ins.sharedTiFlashClient(), s3_key, std::pair{read_file_info_.read_merged_offset, read_file_info_.read_merged_size});
    String s;
    s.resize(read_file_info_.read_merged_size);
    auto n = s3_file->read(s.data(), read_file_info_.read_merged_size);
    RUNTIME_CHECK(n == static_cast<Int64>(read_file_info_.read_merged_size), read_file_info_.merged_filename, read_file_info_.read_merged_offset, read_file_info_.read_merged_size, n);
    return s;
}

inline static std::optional<String> readMergedSubFilesFromCachedFile(const S3RandomAccessFile::ReadFileInfo & read_file_info_)
{
    auto s3_key = S3::S3FilenameView::fromKeyWithPrefix(read_file_info_.merged_filename).toFullKey();
    auto cached_file = tryOpenCachedFile(s3_key, read_file_info_.size);
    if (cached_file != nullptr)
    {
        String data;
        data.resize(read_file_info_.read_merged_size);
        auto n = cached_file->pread(data.data(), read_file_info_.read_merged_size, read_file_info_.read_merged_offset);
        RUNTIME_CHECK(n == static_cast<Int64>(read_file_info_.read_merged_size), read_file_info_.merged_filename, read_file_info_.read_merged_size, n);
        return data;
    }
    else
    {
        return std::nullopt;
    }
}

inline static String readMergedSubfiles(const S3RandomAccessFile::ReadFileInfo & read_file_info_)
{
    if (read_file_info_.read_merged_size == 0)
    {
        return {};
    }

    auto data_from_cache = readMergedSubFilesFromCachedFile(read_file_info_);
    if (data_from_cache)
    {
        return *data_from_cache;
    }
    return readMergedSubFilesFromS3(read_file_info_);
}

inline static RandomAccessFilePtr createFromMergedFile(const String & remote_fname, const S3RandomAccessFile::ReadFileInfo & read_file_info_)
{
    return std::make_shared<MemoryRandomAccessFile>(remote_fname, readMergedSubfiles(read_file_info_));
}

RandomAccessFilePtr S3RandomAccessFile::create(const String & remote_fname)
{
    bool read_from_merged_file = read_file_info && !read_file_info->merged_filename.empty();
    if (read_from_merged_file)
    {
        return createFromMergedFile(remote_fname, *read_file_info);
    }
    else
    {
        return createFromNormalFile(remote_fname, read_file_info ? std::optional<UInt64>(read_file_info->size) : std::nullopt);
    }
}
} // namespace DB::S3
