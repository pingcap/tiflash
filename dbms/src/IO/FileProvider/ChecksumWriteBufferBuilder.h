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
#include <IO/Buffer/WriteBufferFromFileBase.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/FileProvider/FileProvider.h>

#include <string>

namespace DB
{

class ChecksumWriteBufferBuilder
{
public:
    static std::unique_ptr<WriteBufferFromFileBase> build(
        bool has_checksum,
        const FileProviderPtr & file_provider,
        const std::string & filename_,
        const EncryptionPath & encryption_path_,
        bool create_new_encryption_info_,
        const WriteLimiterPtr & write_limiter_,
        ChecksumAlgo checksum_algorithm,
        size_t checksum_frame_size,
        int flags_ = -1,
        mode_t mode = 0666,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    static std::unique_ptr<WriteBufferFromFileBase> build(
        WriteBufferFromWritableFilePtr & writer_buffer,
        ChecksumAlgo checksum_algorithm,
        size_t checksum_frame_size);
};

} // namespace DB
