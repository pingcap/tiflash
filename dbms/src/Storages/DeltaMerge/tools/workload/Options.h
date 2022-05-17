// Copyright 2022 PingCAP, Ltd.
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

#include <string>
#include <vector>

namespace DB::DM::tests
{
struct WorkloadOptions
{
    uint64_t max_key_count;
    std::string write_key_distribution;
    uint64_t write_count;
    uint64_t write_thread_count;

    std::string table;
    std::string pk_type;
    uint64_t columns_count;

    std::vector<std::string> failpoints;

    uint64_t max_write_per_sec;

    std::string log_file;
    std::string log_level;

    bool verification;
    uint64_t verify_round;

    uint64_t random_kill;
    uint64_t max_sleep_sec;

    std::vector<std::string> work_dirs;
    std::string config_file;

    uint64_t read_thread_count;
    uint64_t read_stream_count;

    std::string testing_type;

    bool log_write_request;

    bool enable_ps_v3;

    uint64_t bg_thread_count;

    int64_t table_id;
    std::string table_name;

    std::string toString(std::string seperator = "\n") const;
    std::pair<bool, std::string> parseOptions(int argc, char * argv[]);
    void initFailpoints() const;
};
} // namespace DB::DM::tests
