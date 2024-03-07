// Copyright 2024 PingCAP, Inc.
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

#include <IO/FileProvider/ChecksumReadBufferBuilder.h>
#include <IO/FileProvider/CompressedReadBufferFromFileBuilder.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>


namespace DB
{

std::unique_ptr<LegacyCompressedReadBufferFromFile> CompressedReadBufferFromFileBuilder::buildLegacy(
    FileProviderPtr & file_provider,
    const std::string & path,
    const EncryptionPath & encryption_path,
    const ReadLimiterPtr & read_limiter_,
    size_t buf_size)
{
    auto file_in = ReadBufferFromRandomAccessFileBuilder::buildPtr(
        file_provider,
        path,
        encryption_path,
        buf_size,
        read_limiter_);
    // with legacy checksum in CompressedReadBuffer
    return std::make_unique<CompressedReadBufferFromFileImpl<true>>(std::move(file_in));
}

std::unique_ptr<CompressedReadBufferFromFile> CompressedReadBufferFromFileBuilder::build(
    String && data,
    const String & file_name,
    size_t estimated_size,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size)
{
    auto file_in = ChecksumReadBufferBuilder::build(
        std::move(data),
        file_name,
        estimated_size,
        checksum_algorithm,
        checksum_frame_size);
    return std::make_unique<CompressedReadBufferFromFileImpl<false>>(std::move(file_in));
}

std::unique_ptr<CompressedReadBufferFromFile> CompressedReadBufferFromFileBuilder::build(
    FileProviderPtr & file_provider,
    const std::string & path,
    const EncryptionPath & encryption_path,
    size_t estimated_size,
    const ReadLimiterPtr & read_limiter_,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size)
{
    auto file_in = ChecksumReadBufferBuilder::build(
        file_provider,
        path,
        encryption_path,
        estimated_size,
        read_limiter_,
        checksum_algorithm,
        checksum_frame_size);
    return std::make_unique<CompressedReadBufferFromFileImpl<false>>(std::move(file_in));
}

} // namespace DB