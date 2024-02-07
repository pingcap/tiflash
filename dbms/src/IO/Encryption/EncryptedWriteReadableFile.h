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

#include <IO/BaseFile/WriteReadableFile.h>
#include <IO/Encryption/AESCTRCipherStream.h>
#include <common/types.h>


namespace DB
{
class EncryptedWriteReadableFile : public WriteReadableFile
{
public:
    EncryptedWriteReadableFile(WriteReadableFilePtr & file_, BlockAccessCipherStreamPtr stream_)
        : file{file_}
        , stream{std::move(stream_)}
    {}

    ~EncryptedWriteReadableFile() override = default;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    void close() override { file->close(); }

    int fsync() override { return file->fsync(); }

    int ftruncate(off_t length) override { return file->ftruncate(length); }

    int getFd() const override { return file->getFd(); }

    bool isClosed() const override { return file->isClosed(); }

    String getFileName() const override { return file->getFileName(); }

private:
    WriteReadableFilePtr file;
    BlockAccessCipherStreamPtr stream;
};

} // namespace DB