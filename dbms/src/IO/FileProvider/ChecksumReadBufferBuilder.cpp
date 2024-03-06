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

#include <IO/BaseFile/MemoryRandomAccessFile.h>
#include <IO/Checksum/ChecksumBuffer.h>
#include <IO/FileProvider/ChecksumReadBufferBuilder.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<ReadBufferFromFileBase> ChecksumReadBufferBuilder::build(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    size_t estimated_size,
    const ReadLimiterPtr & read_limiter,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size,
    int flags_)
{
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


std::unique_ptr<ReadBufferFromFileBase> ChecksumReadBufferBuilder::build(
    String && data,
    const String & file_name,
    size_t estimated_size,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size)
{
    auto file = std::make_shared<MemoryRandomAccessFile>(file_name, std::forward<String>(data));
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
