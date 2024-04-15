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

namespace DB
{
class WritableFile
{
public:
    virtual ~WritableFile() = default;

    // Write N bytes of buf to file.  Return the number written, or -1.
    virtual ssize_t write(char * buf, size_t size) = 0;

    virtual ssize_t pwrite(char * buf, size_t size, off_t offset) const = 0;

    virtual off_t seek(off_t offset, int whence) const = 0;

    virtual std::string getFileName() const = 0;

    virtual int getFd() const = 0;

    virtual void open() = 0;

    virtual void close() = 0;

    virtual bool isClosed() const = 0;

    virtual int fsync() = 0;

    virtual int ftruncate(off_t length) = 0;

    // Create a new hard link file for `existing_file` to this file
    // Note that it will close the file descriptor if it is opened.
    virtual void hardLink(const std::string & existing_file) = 0;
};

using WritableFilePtr = std::shared_ptr<WritableFile>;

} // namespace DB
