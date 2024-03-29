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
#include <IO/Buffer/ReadBufferFromFileDescriptor.h>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
/** Accepts path to file and opens it, or pre-opened file descriptor.
  * Closes file by himself (thus "owns" a file descriptor).
  */
class ReadBufferFromFile : public ReadBufferFromFileDescriptor
{
protected:
    std::string file_name;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

public:
    explicit ReadBufferFromFile(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    /// Use pre-opened file descriptor.
    explicit ReadBufferFromFile(
        int fd,
        const std::string & original_file_name = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ReadBufferFromFile(ReadBufferFromFile &&) = default;

    ~ReadBufferFromFile() override;

    /// Close file before destruction of object.
    void close();

    std::string getFileName() const override { return file_name; }
};

} // namespace DB
