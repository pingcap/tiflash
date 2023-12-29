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
#include <Encryption/RandomAccessFile.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
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
class S3RandomAccessFile final : public RandomAccessFile
{
public:
    static RandomAccessFilePtr create(const String & remote_fname);

    S3RandomAccessFile(
        std::shared_ptr<TiFlashS3Client> client_ptr_,
        const String & remote_fname_,
        std::optional<std::pair<UInt64, UInt64>> offset_and_size_ = std::nullopt);

    // Can only seek forward.
    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    std::string getFileName() const override;

    ssize_t pread(char * /*buf*/, size_t /*size*/, off_t /*offset*/) const override
    {
        throw Exception("S3RandomAccessFile not support pread", ErrorCodes::NOT_IMPLEMENTED);
    }

    int getFd() const override
    {
        return -1;
    }

    bool isClosed() const override
    {
        return is_close;
    }

    void close() override
    {
        is_close = true;
    }

    struct ReadFileInfo
    {
        UInt64 size = 0; // File size of `remote_fname` or `merged_filename`, mainly used for FileCache.
        String merged_filename; // If `merged_filename` is not empty, data should read from `merged_filename`.
        UInt64 read_merged_offset = 0;
        UInt64 read_merged_size = 0;
    };

    [[nodiscard]] static auto setReadFileInfo(ReadFileInfo && read_file_info_)
    {
        read_file_info = std::move(read_file_info_);
        return ext::make_scope_guard([]() {
            read_file_info.reset();
        });
    }

private:
    bool initialize();
    off_t seekImpl(off_t offset, int whence);
    ssize_t readImpl(char * buf, size_t size);
    String readRangeOfObject();

    // When reading, it is necessary to pass the extra information of file, such file size, the merged file information to S3RandomAccessFile::create.
    // It is troublesome to pass parameters layer by layer. So currently, use thread_local global variable to pass parameters.
    // TODO: refine these codes later.
    inline static thread_local std::optional<ReadFileInfo> read_file_info;

    std::shared_ptr<TiFlashS3Client> client_ptr;
    String remote_fname;
    // We may merge several small files into a S3 object.
    // `offset_and_size_in_object` is the offset and size of current file in S3 Object.
    // And `cur_offset` is the offset in the current file.
    std::optional<std::pair<UInt64, UInt64>> offset_and_size_in_object;
    off_t cur_offset;
    Aws::S3::Model::GetObjectResult read_result;
    Int64 content_length = 0;

    DB::LoggerPtr log;
    bool is_close = false;

    Int32 cur_retry = 0;
    static constexpr Int32 max_retry = 3;
};

} // namespace DB::S3
