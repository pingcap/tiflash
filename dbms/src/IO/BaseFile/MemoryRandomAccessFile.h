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

#include <Common/Exception.h>
#include <IO/BaseFile/RandomAccessFile.h>
#include <common/likely.h>
#include <common/types.h>

namespace DB
{
class MemoryRandomAccessFile : public RandomAccessFile
{
public:
    MemoryRandomAccessFile(const String & filename_, String && s_)
        : filename(filename_)
        , current_offset(0)
        , s(std::move(s_))
        , is_closed(false)
    {}

    ~MemoryRandomAccessFile() override = default;

    off_t seek(off_t offset, int whence) override
    {
        RUNTIME_CHECK(whence == SEEK_SET, whence);
        RUNTIME_CHECK(offset >= 0 && offset <= static_cast<off_t>(s.size()), offset, s.size());
        current_offset = offset;
        return current_offset;
    }

    ssize_t read(char * buf, size_t size) override
    {
        auto n = std::min(s.size() - current_offset, size);
        memcpy(buf, s.data() + current_offset, n);
        current_offset += n;
        return n;
    }

    ssize_t pread(char * buf, size_t size, off_t offset) const override
    {
        RUNTIME_CHECK(offset >= 0 && offset <= static_cast<off_t>(s.size()), offset, s.size());
        auto n = std::min(s.size() - offset, size);
        memcpy(buf, s.data() + offset, n);
        return n;
    }

    std::string getFileName() const override { return filename; }
    std::string getInitialFileName() const override { return filename; }

    int getFd() const override { return -1; }

    bool isClosed() const override { return is_closed; }

    void close() override { is_closed = true; }

private:
    String filename;
    off_t current_offset;
    String s;
    bool is_closed;
};

} // namespace DB
