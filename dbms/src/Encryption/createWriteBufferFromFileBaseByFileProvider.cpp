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

#include <Encryption/WriteBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(_MSC_VER)
#include <IO/WriteBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>
#include <IO/ChecksumBuffer.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    size_t estimated_size,
    size_t aio_threshold,
    size_t buffer_size_,
    int flags_,
    mode_t mode,
    char * existing_memory_,
    size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        return std::make_unique<WriteBufferFromFileProvider>(
            file_provider,
            filename_,
            encryption_path_,
            create_new_encryption_info_,
            write_limiter_,
            buffer_size_,
            flags_,
            mode,
            existing_memory_,
            alignment);
    }
    else
    {
        // TODO: support encryption when AIO enabled
        throw Exception("AIO is not implemented when create file using FileProvider", ErrorCodes::NOT_IMPLEMENTED);
    }
}

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


std::unique_ptr<WriteBufferFromFileBase> createWriteBufferFromFileBaseByWriterBuffer(
    std::unique_ptr<WriteBufferFromWritableFile> & writer_buffer,
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
