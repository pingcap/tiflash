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

#include <IO/BaseFile/RandomAccessFile.h>
#include <IO/Encryption/AESCTRCipherStream.h>

#include <string>

namespace DB
{
class EncryptedRandomAccessFile : public RandomAccessFile
{
public:
    EncryptedRandomAccessFile(RandomAccessFilePtr & file_, BlockAccessCipherStreamPtr stream_)
        : file{file_}
        , file_offset{0}
        , stream{std::move(stream_)}
    {}

    ~EncryptedRandomAccessFile() override = default;

    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFd() const override { return file->getFd(); }

    bool isClosed() const override { return file->isClosed(); }

    void close() override;

private:
    RandomAccessFilePtr file;

    off_t file_offset;

    BlockAccessCipherStreamPtr stream;
};

} // namespace DB
