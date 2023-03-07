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

#include <Encryption/FileProvider.h>
#include <Encryption/WriteBufferFromFileProvider.h>

namespace DB
{
WriteBufferFromFileProvider::WriteBufferFromFileProvider(
    const FileProviderPtr & file_provider_,
    const std::string & file_name_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    size_t buf_size,
    int flags,
    mode_t mode,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromWritableFile(
        file_provider_->newWritableFile(file_name_, encryption_path_, true, create_new_encryption_info_, write_limiter_, flags, mode),
        buf_size,
        existing_memory,
        alignment)
{}
} // namespace DB
