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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <IO/BaseFile/RandomAccessFile.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <aws/s3/model/GetObjectResult.h>
#include <common/types.h>

#include <ext/scope_guard.h>

/// Remove the population of thread_local from Poco
#ifdef thread_local
#undef thread_local
#endif

namespace DB::S3
{
class TiFlashS3Client;
}

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::S3
{
struct PrefetchCache
{
    using ReadFunc = std::function<ssize_t(char *, size_t)>;

    PrefetchCache(UInt32 hit_limit_, ReadFunc read_func_, size_t buffer_size_)
        : hit_limit(hit_limit_)
        , hit_count(0)
        , read_func(read_func_)
        , buffer_size(buffer_size_)
    {
        pos = buffer_size;
    }

    // - If the read is filled entirely by cache, then we will return the "gcount" of the cache.
    // - If the read is filled by both the cache and `read_func`,
    //   + If the read_func returns a positive number, we will add the contribution of the cache, and then return.
    //   + Otherwise, we will return what `read)func` returns.
    ssize_t read(char * buf, size_t size)
    {
        if (hit_count++ < hit_limit)
        {
            // Do not use the cache.
            return read_func(buf, size);
        }
        maybePrefetch();
        if (pos + size > buffer_limit)
        {
            // LOG_INFO(
            //     DB::Logger::get(),
            //     "!!!! try read {} from cache + direct buffer_limit={} pos={}",
            //     size,
            //     buffer_limit,
            //     pos);
            // No enough data in cache.
            ::memcpy(buf, write_buffer.data() + pos, buffer_limit);
            auto read_from_cache = buffer_limit - pos;
            cache_read += read_from_cache;
            pos = buffer_limit;
            auto expected_direct_read_bytes = size - read_from_cache;
            auto res = read_func(buf + read_from_cache, expected_direct_read_bytes);
            // LOG_INFO(
            //     DB::Logger::get(),
            //     "!!!! refill pos={} size={} buffer_size={} buffer_limit={} expected_direct_read_bytes={} "
            //     "read_from_cache={} res={} direct={}",
            //     pos,
            //     size,
            //     buffer_size,
            //     buffer_limit,
            //     expected_direct_read_bytes,
            //     read_from_cache,
            //     res,
            //     direct_read);
            if (res < 0)
                return res;
            direct_read += res;
            // We may not read `size` data.
            // LOG_INFO(DB::Logger::get(), "!!!! result res={} buffer_limit={} pos={}", res, buffer_limit, pos);
            return res + read_from_cache;
        }
        else
        {
            // LOG_INFO(DB::Logger::get(), "!!!! try read {} from cache", size);
            ::memcpy(buf, write_buffer.data() + pos, size);
            cache_read += size;
            pos += size;
            return size;
        }
    }

    size_t skip(size_t ignore_count)
    {
        if (hit_count++ < hit_limit)
        {
            return ignore_count;
        }
        maybePrefetch();
        if (pos + ignore_count > buffer_limit)
        {
            // No enough data in cache.
            auto read_from_cache = buffer_limit - pos;
            pos = buffer_limit;
            auto expected_direct_read_bytes = ignore_count - read_from_cache;
            return expected_direct_read_bytes;
        }
        else
        {
            pos += ignore_count;
            return 0;
        }
    }

    enum class PrefetchRes
    {
        NeedNot,
        Ok,
    };

    PrefetchRes maybePrefetch()
    {
        if (eof)
        {
            return PrefetchRes::NeedNot;
        }
        if (pos >= buffer_size)
        {
            write_buffer.reserve(buffer_size);
            // TODO Check if it is OK to read when the rest of the chars are less than size.
            auto res = read_func(write_buffer.data(), buffer_size);
            // LOG_INFO(DB::Logger::get(), "!!!! prefetch res res={}", res);
            if (res < 0)
            {
                // Error state.
                eof = true;
            }
            else
            {
                // If we actually got some data.
                pos = 0;
                buffer_limit = res;
            }
        }
        return PrefetchRes::NeedNot;
    }

    size_t getCacheRead() const { return cache_read; }
    size_t getDirectRead() const { return direct_read; }
    bool needsRefill() const { return pos >= buffer_limit; }

private:
    UInt32 hit_limit;
    std::atomic<UInt32> hit_count;
    bool eof = false;
    ReadFunc read_func;
    // Equal to size of `write_buffer`.
    size_t buffer_size;
    size_t pos;
    // How many data is actually in the buffer.
    size_t buffer_limit;
    std::vector<char> write_buffer;
    size_t direct_read = 0;
    size_t cache_read = 0;
};
class S3RandomAccessFile final : public RandomAccessFile
{
public:
    static RandomAccessFilePtr create(const String & remote_fname);

    S3RandomAccessFile(std::shared_ptr<TiFlashS3Client> client_ptr_, const String & remote_fname_);

    // Can only seek forward.
    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    std::string getFileName() const override;

    ssize_t pread(char * /*buf*/, size_t /*size*/, off_t /*offset*/) const override
    {
        throw Exception("S3RandomAccessFile not support pread", ErrorCodes::NOT_IMPLEMENTED);
    }

    int getFd() const override { return -1; }

    bool isClosed() const override { return is_close; }

    void close() override { is_close = true; }

    struct ReadFileInfo
    {
        UInt64 size = 0; // File size of `remote_fname` or `merged_filename`, mainly used for FileCache.
        DB::DM::ScanContextPtr scan_context;
    };

    [[nodiscard]] static auto setReadFileInfo(ReadFileInfo && read_file_info_)
    {
        read_file_info = std::move(read_file_info_);
        return ext::make_scope_guard([]() { read_file_info.reset(); });
    }

private:
    bool initialize();
    off_t seekImpl(off_t offset, int whence);
    ssize_t readImpl(char * buf, size_t size);
    String readRangeOfObject();

    // When reading, it is necessary to pass the extra information of file, such file size, to S3RandomAccessFile::create.
    // It is troublesome to pass parameters layer by layer. So currently, use thread_local global variable to pass parameters.
    // TODO: refine these codes later.
    inline static thread_local std::optional<ReadFileInfo> read_file_info;

    std::shared_ptr<TiFlashS3Client> client_ptr;
    String remote_fname;

    off_t cur_offset;
    Aws::S3::Model::GetObjectResult read_result;
    Int64 content_length = 0;

    DB::LoggerPtr log;
    bool is_close = false;

    Int32 cur_retry = 0;
    static constexpr Int32 max_retry = 3;

    std::unique_ptr<PrefetchCache> prefetch;
};

} // namespace DB::S3
