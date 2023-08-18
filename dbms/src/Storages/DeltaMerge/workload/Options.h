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

#include <Storages/Page/PageStorage.h>

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

    PageStorageRunMode ps_run_mode;

    uint64_t bg_thread_count;

    int64_t table_id;
    std::string table_name;

    bool is_fast_scan;

    bool enable_read_thread;

    String s3_bucket;
    String s3_endpoint;
    String s3_access_key_id;
    String s3_secret_access_key;
    String s3_root;
    UInt64 s3_put_concurrency;
    UInt64 s3_get_concurrency;
    UInt64 s3_put_count_per_thread;
    UInt64 s3_get_count_per_thread;
    String s3_temp_dir;

    std::string toString(std::string seperator = "\n") const;
    std::pair<bool, std::string> parseOptions(int argc, char * argv[]);
    void initFailpoints() const;
};
} // namespace DB::DM::tests
