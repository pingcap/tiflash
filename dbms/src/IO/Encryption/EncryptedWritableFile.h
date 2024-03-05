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

#include <IO/BaseFile/WritableFile.h>
#include <IO/Encryption/AESCTRCipherStream.h>

#include <string>

namespace DB
{
class EncryptedWritableFile : public WritableFile
{
public:
    EncryptedWritableFile(WritableFilePtr & file_, BlockAccessCipherStreamPtr stream_)
        : file{file_}
        , file_offset{0}
        , stream{std::move(stream_)}
    {}

    ~EncryptedWritableFile() override = default;

    ssize_t write(char * buf, size_t size) override;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    off_t seek(off_t offset, int whence) const override;

    std::string getFileName() const override { return file->getFileName(); }

    int getFd() const override { return file->getFd(); }

    void open() override;

    void close() override;

    bool isClosed() const override { return file->isClosed(); }

    int fsync() override { return file->fsync(); }

    int ftruncate(off_t length) override { return file->ftruncate(length); }

    void hardLink(const std::string & existing_file) override { file->hardLink(existing_file); };

private:
    WritableFilePtr file;
    // logic file_offset for EncryptedWritableFile, should be same as the underlying plaintext file
    off_t file_offset;

    BlockAccessCipherStreamPtr stream;
};

} // namespace DB
