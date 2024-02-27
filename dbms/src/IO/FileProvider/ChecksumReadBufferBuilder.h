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
#include <IO/Buffer/ReadBufferFromFileBase.h>
#include <IO/FileProvider/FileProvider.h>


namespace DB
{

class ChecksumReadBufferBuilder
{
public:
    /// @attention: estimated_size should be at least DBMS_DEFAULT_BUFFER_SIZE if one want to do seeking; however, if one knows that target file
    /// only consists of a single small frame, one can use a smaller estimated_size to reduce memory footprint.
    static std::unique_ptr<ReadBufferFromFileBase> build(
        const FileProviderPtr & file_provider,
        const std::string & filename_,
        const EncryptionPath & encryption_path_,
        size_t estimated_size,
        const ReadLimiterPtr & read_limiter,
        ChecksumAlgo checksum_algorithm,
        size_t checksum_frame_size,
        int flags_ = -1);

    static std::unique_ptr<ReadBufferFromFileBase> build(
        String && data,
        const String & file_name,
        size_t estimated_size,
        ChecksumAlgo checksum_algorithm,
        size_t checksum_frame_size);
};

} // namespace DB
