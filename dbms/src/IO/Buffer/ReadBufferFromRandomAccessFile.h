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

#include <IO/Buffer/ReadBufferFromFileDescriptor.h>

namespace DB
{
class RandomAccessFile;
using RandomAccessFilePtr = std::shared_ptr<RandomAccessFile>;

class ReadBufferFromRandomAccessFile : public ReadBufferFromFileDescriptor
{
protected:
    bool nextImpl() override;

public:
    explicit ReadBufferFromRandomAccessFile(
        RandomAccessFilePtr file_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~ReadBufferFromRandomAccessFile() override;

    std::string getFileName() const override;

    std::string getInitialFileName() const;

    int getFD() const override;

private:
    off_t doSeekInFile(off_t offset, int whence) override;

private:
    RandomAccessFilePtr file;
};
using ReadBufferFromRandomAccessFilePtr = std::shared_ptr<ReadBufferFromRandomAccessFile>;
using ReadBufferFromRandomAccessFileUPtr = std::unique_ptr<ReadBufferFromRandomAccessFile>;
} // namespace DB
