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

#pragma once

#include <Encryption/FileProvider.h>
#include <Encryption/WritableFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>

namespace DB
{
class WriteBufferFromFileProvider : public WriteBufferFromFileDescriptor
{
protected:
    void nextImpl() override;

public:
    WriteBufferFromFileProvider(
        const FileProviderPtr & file_provider_,
        const std::string & file_name_,
        const EncryptionPath & encryption_path,
        bool create_new_encryption_info_ = true,
        const WriteLimiterPtr & write_limiter_ = nullptr,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromFileProvider() override;

    void close() override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFD() const override { return file->getFd(); }

private:
    WritableFilePtr file;
};

} // namespace DB
