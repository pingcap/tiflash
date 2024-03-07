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

#include <IO/Checksum/ChecksumBuffer.h>
#include <IO/FileProvider/ChecksumWriteBufferBuilder.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{
std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size,
    int flags_,
    mode_t mode)
{
    auto file_ptr = file_provider->newWritableFile(
        filename_,
        encryption_path_,
        true,
        create_new_encryption_info_,
        write_limiter_,
        flags_,
        mode);
    switch (checksum_algorithm)
    {
    case ChecksumAlgo::None:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::None>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::CRC32:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::CRC32>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::CRC64:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::CRC64>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::City128:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::City128>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::XXH3:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::XXH3>>(file_ptr, checksum_frame_size);
    }
    throw Exception("error creating framed checksum buffer instance: checksum unrecognized");
}
} // namespace

std::unique_ptr<WriteBufferFromFileBase> ChecksumWriteBufferBuilder::build(
    bool has_checksum,
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size,
    int flags_,
    mode_t mode,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
{
    if (has_checksum)
    {
        return createWriteBufferFromFileBaseByFileProvider(
            file_provider,
            filename_,
            encryption_path_,
            create_new_encryption_info_,
            write_limiter_,
            checksum_algorithm,
            checksum_frame_size,
            flags_,
            mode);
    }
    else
    {
        return WriteBufferFromWritableFileBuilder::buildPtr(
            file_provider,
            filename_,
            encryption_path_,
            create_new_encryption_info_,
            write_limiter_,
            buf_size,
            flags_,
            mode,
            existing_memory,
            alignment);
    }
}

std::unique_ptr<WriteBufferFromFileBase> ChecksumWriteBufferBuilder::build(
    WriteBufferFromWritableFilePtr & writer_buffer,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size)
{
    auto file_ptr = writer_buffer->file;
    switch (checksum_algorithm)
    {
    case ChecksumAlgo::None:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::None>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::CRC32:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::CRC32>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::CRC64:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::CRC64>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::City128:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::City128>>(file_ptr, checksum_frame_size);
    case ChecksumAlgo::XXH3:
        return std::make_unique<FramedChecksumWriteBuffer<Digest::XXH3>>(file_ptr, checksum_frame_size);
    }
    throw Exception("error creating framed checksum buffer instance: checksum unrecognized");
}

} // namespace DB
