// Copyright 2022 PingCAP, Ltd.
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

#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>

#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(_MSC_VER)
#include <IO/ReadBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>
#include <IO/ChecksumBuffer.h>
namespace ProfileEvents
{
extern const Event CreatedReadBufferOrdinary;
}

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBaseByFileProvider(
    FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    size_t estimated_size,
    size_t aio_threshold,
    const ReadLimiterPtr & read_limiter,
    size_t buffer_size_,
    int flags_,
    char * existing_memory_,
    size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
        return std::make_unique<ReadBufferFromFileProvider>(
            file_provider,
            filename_,
            encryption_path_,
            buffer_size_,
            read_limiter,
            flags_,
            existing_memory_,
            alignment);
    }
    else
    {
        // TODO: support encryption when AIO enabled
        throw Exception("AIO is not implemented when create file using FileProvider", ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::unique_ptr<ReadBufferFromFileBase>
createReadBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    size_t estimated_size,
    const ReadLimiterPtr & read_limiter,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size,
    int flags_)
{
    ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
    auto file = file_provider->newRandomAccessFile(filename_, encryption_path_, read_limiter, flags_);
    auto allocation_size = std::min(estimated_size, checksum_frame_size);
    switch (checksum_algorithm)
    {
    case ChecksumAlgo::None:
        return std::make_unique<FramedChecksumReadBuffer<Digest::None>>(file, allocation_size);
    case ChecksumAlgo::CRC32:
        return std::make_unique<FramedChecksumReadBuffer<Digest::CRC32>>(file, allocation_size);
    case ChecksumAlgo::CRC64:
        return std::make_unique<FramedChecksumReadBuffer<Digest::CRC64>>(file, allocation_size);
    case ChecksumAlgo::City128:
        return std::make_unique<FramedChecksumReadBuffer<Digest::City128>>(file, allocation_size);
    case ChecksumAlgo::XXH3:
        return std::make_unique<FramedChecksumReadBuffer<Digest::XXH3>>(file, allocation_size);
    }
    throw Exception("error creating framed checksum buffer instance: checksum unrecognized");
}

} // namespace DB
