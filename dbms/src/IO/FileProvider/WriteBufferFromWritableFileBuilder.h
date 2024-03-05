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

#include <IO/BaseFile/fwd.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/FileProvider/EncryptionPath.h>
#include <IO/FileProvider/FileProvider_fwd.h>

namespace DB
{

class WriteBufferFromWritableFileBuilder
{
public:
    static WriteBufferFromWritableFilePtr buildPtr(
        const FileProviderPtr & file_provider,
        const std::string & file_name_,
        const EncryptionPath & encryption_path,
        bool create_new_encryption_info_ = true,
        const WriteLimiterPtr & write_limiter_ = nullptr,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    static WriteBufferFromWritableFile build(
        const FileProviderPtr & file_provider,
        const std::string & file_name_,
        const EncryptionPath & encryption_path,
        bool create_new_encryption_info_ = true,
        const WriteLimiterPtr & write_limiter_ = nullptr,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);
};

} // namespace DB
