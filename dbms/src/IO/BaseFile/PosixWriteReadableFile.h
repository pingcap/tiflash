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
#include <IO/BaseFile/WriteReadableFile.h>
#include <common/types.h>

namespace CurrentMetrics
{
extern const Metric OpenFileForReadWrite;
} // namespace CurrentMetrics

namespace DB
{
class PosixWriteReadableFile : public WriteReadableFile
{
public:
    PosixWriteReadableFile(
        const String & file_name_,
        bool truncate_when_exists_,
        int flags,
        mode_t mode,
        const WriteLimiterPtr & write_limiter_ = nullptr,
        const ReadLimiterPtr & read_limiter_ = nullptr);

    ~PosixWriteReadableFile() override;

    ssize_t pwrite(char * buf, size_t size, off_t offset) const override;

    int getFd() const override { return fd; }

    bool isClosed() const override { return fd == -1; }

    int fsync() override;

    int ftruncate(off_t length) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    String getFileName() const override { return file_name; }

    void close() override;

private:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForReadWrite, 0};

    String file_name;
    int fd;
    WriteLimiterPtr write_limiter;
    ReadLimiterPtr read_limiter;
};
} // namespace DB