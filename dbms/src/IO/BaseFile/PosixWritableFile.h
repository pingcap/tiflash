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

#include <Common/CurrentMetrics.h>
#include <IO/BaseFile/RateLimiter.h>
#include <IO/BaseFile/WritableFile.h>

#include <string>

namespace CurrentMetrics
{
extern const Metric OpenFileForWrite;
}

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{
class PosixWritableFile : public WritableFile
{
public:
    PosixWritableFile(
        const std::string & file_name_,
        bool truncate_when_exists_,
        int flags,
        mode_t mode,
        const WriteLimiterPtr & write_limiter_ = nullptr);

    ~PosixWritableFile() override;

    ssize_t write(char * buf, size_t size) override;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    off_t seek(off_t offset, int whence) const override;

    std::string getFileName() const override { return file_name; }

    int getFd() const override { return fd; }

    void open() override;

    void close() override;

    bool isClosed() const override { return fd == -1; }

    int fsync() override;

    int ftruncate(off_t length) override;

    void hardLink(const std::string & existing_file) override;

private:
    void doOpenFile(bool truncate_when_exists_, int flags, mode_t mode);

private:
    // Only add metrics when file is actually added in `doOpenFile`.
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite, 0};
    std::string file_name;
    int fd = -1;
    WriteLimiterPtr write_limiter;
};

} // namespace DB
