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

#pragma once

#include <Common/Checksum.h>
#include <IO/Compression/CompressedReadBufferFromFile.h>
#include <IO/FileProvider/FileProvider.h>

namespace DB
{

class CompressedReadBufferFromFileBuilder
{
public:
    static std::unique_ptr<LegacyCompressedReadBufferFromFile> buildLegacy(
        const FileProviderPtr & file_provider,
        const std::string & path,
        const EncryptionPath & encryption_path,
        const ReadLimiterPtr & read_limiter_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    static std::unique_ptr<CompressedReadBufferFromFile> build(
        String && data,
        const String & file_name,
        size_t estimated_size,
        ChecksumAlgo checksum_algorithm,
        size_t checksum_frame_size);

    static std::unique_ptr<CompressedReadBufferFromFile> build(
        const FileProviderPtr & file_provider,
        const std::string & path,
        const EncryptionPath & encryption_path,
        size_t estimated_size,
        const ReadLimiterPtr & read_limiter_,
        ChecksumAlgo checksum_algorithm,
        size_t checksum_frame_size);
};

} // namespace DB
