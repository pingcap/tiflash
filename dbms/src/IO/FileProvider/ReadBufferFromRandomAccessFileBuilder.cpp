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

#include <IO/FileProvider/FileProvider.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>


namespace DB
{

ReadBufferFromRandomAccessFileUPtr ReadBufferFromRandomAccessFileBuilder::buildPtr(
    const FileProviderPtr & file_provider,
    const std::string & file_name_,
    const EncryptionPath & encryption_path_,
    size_t buf_size,
    const ReadLimiterPtr & read_limiter,
    int flags,
    char * existing_memory,
    size_t alignment)
{
    auto file = file_provider->newRandomAccessFile(file_name_, encryption_path_, read_limiter, flags);
    return std::make_unique<ReadBufferFromRandomAccessFile>(file, buf_size, existing_memory, alignment);
}

ReadBufferFromRandomAccessFilePtr ReadBufferFromRandomAccessFileBuilder::buildSharedPtr(
    const FileProviderPtr & file_provider,
    const std::string & file_name_,
    const EncryptionPath & encryption_path_,
    size_t buf_size,
    const ReadLimiterPtr & read_limiter,
    int flags,
    char * existing_memory,
    size_t alignment)
{
    auto file = file_provider->newRandomAccessFile(file_name_, encryption_path_, read_limiter, flags);
    return std::make_shared<ReadBufferFromRandomAccessFile>(file, buf_size, existing_memory, alignment);
}

ReadBufferFromRandomAccessFile ReadBufferFromRandomAccessFileBuilder::build(
    const FileProviderPtr & file_provider,
    const std::string & file_name_,
    const EncryptionPath & encryption_path_,
    size_t buf_size,
    const ReadLimiterPtr & read_limiter,
    int flags,
    char * existing_memory,
    size_t alignment)
{
    auto file = file_provider->newRandomAccessFile(file_name_, encryption_path_, read_limiter, flags);
    return ReadBufferFromRandomAccessFile(file, buf_size, existing_memory, alignment);
}

} // namespace DB
