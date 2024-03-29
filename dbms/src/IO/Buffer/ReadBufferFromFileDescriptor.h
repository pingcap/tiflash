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

#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/ReadBufferFromFileBase.h>
#include <port/unistd.h>


namespace DB
{
/** Use ready file descriptor. Does not open or close a file.
  */
class ReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
protected:
    int fd;
    off_t pos_in_file; /// What offset in file corresponds to working_buffer.end().

    bool nextImpl() override;

    /// Name or some description of file.
    std::string getFileName() const override;

public:
    explicit ReadBufferFromFileDescriptor(
        int fd_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0)
        : ReadBufferFromFileBase(buf_size, existing_memory, alignment)
        , fd(fd_)
        , pos_in_file(0)
    {}

    ReadBufferFromFileDescriptor(ReadBufferFromFileDescriptor &&) = default;

    int getFD() const override { return fd; }

    off_t getPositionInFile() override { return pos_in_file - (working_buffer.end() - pos); }

private:
    /// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
    off_t doSeek(off_t offset, int whence) override;

    virtual off_t doSeekInFile(off_t offset, int whence);

    /// Assuming file descriptor supports 'select', check that we have data to read or wait until timeout.
    bool poll(size_t timeout_microseconds) const;
};

} // namespace DB
