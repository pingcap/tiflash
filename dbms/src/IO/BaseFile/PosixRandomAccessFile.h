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
#include <IO/BaseFile/RandomAccessFile.h>

#include <string>

namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;

class PosixRandomAccessFile : public RandomAccessFile
{
public:
    static RandomAccessFilePtr create(const String & file_name_);

    PosixRandomAccessFile(
        const std::string & file_name_,
        int flags,
        const ReadLimiterPtr & read_limiter_ = nullptr,
        const FileSegmentPtr & file_seg_ = nullptr);

    ~PosixRandomAccessFile() override;

    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file_name; }
    std::string getInitialFileName() const override { return file_name; }

    bool isClosed() const override { return fd == -1; }

    int getFd() const override { return fd; }

    void close() override;

private:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};
    std::string file_name;
    int fd;
    ReadLimiterPtr read_limiter;
    FileSegmentPtr file_seg;
};

} // namespace DB
