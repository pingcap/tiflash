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

#include <sys/types.h>

#include <memory>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{
class RandomAccessFile
{
public:
    virtual ~RandomAccessFile() = default;

    virtual off_t seek(off_t offset, int whence) = 0;

    virtual ssize_t read(char * buf, size_t size) = 0;

    virtual ssize_t pread(char * buf, size_t size, off_t offset) const = 0;

    virtual std::string getFileName() const = 0;

    virtual int getFd() const = 0;

    virtual bool isClosed() const = 0;

    virtual void close() = 0;
};

using RandomAccessFilePtr = std::shared_ptr<RandomAccessFile>;
} // namespace DB
