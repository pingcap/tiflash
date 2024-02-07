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

#include <IO/Buffer/WriteBufferFromFileBase.h>


namespace DB
{
/** Use ready file descriptor. Does not open or close a file.
  */
class WriteBufferFromFileDescriptor : public WriteBufferFromFileBase
{
protected:
    int fd;

    void nextImpl() override;

    /// Name or some description of file.
    std::string getFileName() const override;

public:
    explicit WriteBufferFromFileDescriptor(
        int fd_ = -1,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    /** Could be used before initialization if needed 'fd' was not passed to constructor.
      * It's not possible to change 'fd' during work.
      */
    void setFD(int fd_) { fd = fd_; }

    ~WriteBufferFromFileDescriptor() override;

    int getFD() const override { return fd; }

    off_t getPositionInFile() override;

    void sync() override;

    void close() override;

private:
    off_t doSeek(off_t offset, int whence) override;

    void doTruncate(off_t length) override;
};

} // namespace DB
