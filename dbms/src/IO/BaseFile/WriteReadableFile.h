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
#include <IO/BaseFile/WritableFile.h>
#include <common/types.h>

#include <memory>

namespace DB
{
/**
 * Do not add write/read/seek, This may cause some multi-threaded reading and writing problems
 */
class WriteReadableFile
{
public:
    virtual ~WriteReadableFile() = default;

    virtual ssize_t pwrite(char * buf, size_t size, off_t offset) const = 0;

    virtual ssize_t pread(char * buf, size_t size, off_t offset) const = 0;

    virtual int fsync() = 0;

    virtual int getFd() const = 0;

    virtual bool isClosed() const = 0;

    virtual void close() = 0;

    virtual int ftruncate(off_t length) = 0;

    virtual String getFileName() const = 0;
};

using WriteReadableFilePtr = std::shared_ptr<WriteReadableFile>;

} // namespace DB