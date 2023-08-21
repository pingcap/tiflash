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

#include <Common/Checksum.h>
#include <Common/nocopyable.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromWritableFile.h>

#include <string>

namespace DB
{
std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByWriterBuffer(
    std::unique_ptr<WriteBufferFromWritableFile> & writer_buffer,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size);

/** Create an object to write data to a file.
  * estimated_size - number of bytes to write
  * aio_threshold - the minimum number of bytes for asynchronous writes
  *
  * Caution: (AIO is not supported yet.)
  *
  * If aio_threshold = 0 or estimated_size < aio_threshold, the write operations are executed synchronously.
  * Otherwise, write operations are performed asynchronously.
  */
std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    size_t estimated_size,
    size_t aio_threshold,
    size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
    int flags_ = -1,
    mode_t mode = 0666,
    char * existing_memory_ = nullptr,
    size_t alignment = 0);

std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size,
    int flags_ = -1,
    mode_t mode = 0666);

class WriteBufferByFileProviderBuilder
{
    bool has_checksum;
    const FileProviderPtr & file_provider;
    const std::string & filename;
    const EncryptionPath & encryption_path;
    bool create_new_encryption_info;
    const WriteLimiterPtr & write_limiter;
    int flags = -1;
    mode_t mode = 0666;

    // legacy
    size_t estimated_size = 0;
    size_t aio_threshold = 0;
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t alignment = 0;
    char * existing_memory = nullptr;

    // checksum
    ChecksumAlgo checksum_algorithm = ChecksumAlgo::None;
    size_t checksum_frame_size = DBMS_DEFAULT_BUFFER_SIZE;

public:
    WriteBufferByFileProviderBuilder(
        bool has_checksum,
        const FileProviderPtr & file_provider,
        const std::string & filename,
        const EncryptionPath & encryption_path,
        bool create_new_encryption_info,
        const WriteLimiterPtr & write_limiter)
        : has_checksum(has_checksum)
        , file_provider(file_provider)
        , filename(filename)
        , encryption_path(encryption_path)
        , create_new_encryption_info(create_new_encryption_info)
        , write_limiter(write_limiter)
    {}

    DISALLOW_COPY(WriteBufferByFileProviderBuilder);

    std::unique_ptr<WriteBufferFromFileBase> build()
    {
        if (has_checksum)
        {
            return createWriteBufferFromFileBaseByFileProvider(
                file_provider,
                filename,
                encryption_path,
                create_new_encryption_info,
                write_limiter,
                checksum_algorithm,
                checksum_frame_size,
                flags,
                mode);
        }
        else
        {
            return createWriteBufferFromFileBaseByFileProvider(
                file_provider,
                filename,
                encryption_path,
                create_new_encryption_info,
                write_limiter,
                estimated_size,
                aio_threshold,
                buffer_size,
                flags,
                mode,
                existing_memory,
                alignment);
        }
    };

#define TIFLASH_WBBFPB_CREATE_SETTER(NAME)                            \
    template <class T>                                                \
    WriteBufferByFileProviderBuilder & with_##NAME(T && NAME##_value) \
    {                                                                 \
        (NAME) = NAME##_value;                                        \
        return *this;                                                 \
    }

    TIFLASH_WBBFPB_CREATE_SETTER(flags);

    TIFLASH_WBBFPB_CREATE_SETTER(mode);

    TIFLASH_WBBFPB_CREATE_SETTER(estimated_size);

    TIFLASH_WBBFPB_CREATE_SETTER(aio_threshold);

    TIFLASH_WBBFPB_CREATE_SETTER(buffer_size);

    TIFLASH_WBBFPB_CREATE_SETTER(alignment);

    TIFLASH_WBBFPB_CREATE_SETTER(existing_memory);

    TIFLASH_WBBFPB_CREATE_SETTER(checksum_algorithm);

    TIFLASH_WBBFPB_CREATE_SETTER(checksum_frame_size);
};


#undef TIFLASH_WBBFPB_CREATE_SETTER
} // namespace DB
