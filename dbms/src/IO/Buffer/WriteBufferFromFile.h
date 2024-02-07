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
#include <IO/Buffer/WriteBufferFromFileDescriptor.h>
#include <sys/types.h>


namespace CurrentMetrics
{
extern const Metric OpenFileForWrite;
}


#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{
/** Accepts path to file and opens it, or pre-opened file descriptor.
  * Closes file by himself (thus "owns" a file descriptor).
  */
class WriteBufferFromFile : public WriteBufferFromFileDescriptor
{
protected:
    std::string file_name;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite};

public:
    explicit WriteBufferFromFile(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    /// Use pre-opened file descriptor.
    explicit WriteBufferFromFile(
        int fd,
        const std::string & original_file_name = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromFile() override;

    /// Close file before destruction of object.
    void close() override;

    std::string getFileName() const override { return file_name; }
};

} // namespace DB
