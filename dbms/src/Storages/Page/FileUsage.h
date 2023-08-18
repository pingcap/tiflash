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
#include <stdint.h>

#include <cstddef>

namespace DB
{
struct FileUsageStatistics
{
    // blob file
    size_t total_disk_size = 0;
    size_t total_valid_size = 0;
    size_t total_file_num = 0;

    // log file
    size_t total_log_disk_size = 0;
    size_t total_log_file_num = 0;

    // in-memory
    size_t num_pages = 0;

    FileUsageStatistics & merge(const FileUsageStatistics & rhs)
    {
        total_disk_size += rhs.total_disk_size;
        total_valid_size += rhs.total_valid_size;
        total_file_num += rhs.total_file_num;

        total_log_disk_size += rhs.total_log_disk_size;
        total_log_file_num += rhs.total_log_file_num;

        num_pages += rhs.num_pages;
        return *this;
    }
};

} // namespace DB
