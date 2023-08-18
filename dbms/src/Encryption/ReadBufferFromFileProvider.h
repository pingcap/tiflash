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

#include <Encryption/EncryptionPath.h>
#include <Encryption/FileProvider_fwd.h>
#include <IO/ReadBufferFromRandomAccessFile.h>

namespace DB
{

class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

/**
 * Note: This class maybe removed in the future, use ReadBufferFromRandomAccessFile instead if possible
 */
class ReadBufferFromFileProvider : public ReadBufferFromRandomAccessFile
{
public:
    ReadBufferFromFileProvider(
        const FileProviderPtr & file_provider_,
        const std::string & file_name_,
        const EncryptionPath & encryption_path_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const ReadLimiterPtr & read_limiter = nullptr,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0);
};
} // namespace DB
