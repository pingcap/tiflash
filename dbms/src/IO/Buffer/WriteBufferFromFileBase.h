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

#include <IO/Buffer/BufferWithOwnMemory.h>
#include <IO/Buffer/WriteBuffer.h>
#include <fcntl.h>

#include <string>

namespace DB
{
class WriteBufferFromFileBase : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment);
    ~WriteBufferFromFileBase() override;

    off_t seek(off_t off, int whence = SEEK_SET);
    void truncate(off_t length = 0);
    virtual off_t getPositionInFile() = 0;
    virtual off_t getMaterializedBytes() { return getPositionInFile(); }
    virtual void sync() = 0;
    virtual std::string getFileName() const = 0;
    virtual int getFD() const = 0;
    virtual void close() = 0;

protected:
    virtual off_t doSeek(off_t off, int whence) = 0;
    virtual void doTruncate(off_t length) = 0;
};

} // namespace DB
