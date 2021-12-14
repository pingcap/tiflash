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

    std::string toString() const;
    std::pair<bool, std::string> parseOptions(int argc, char * argv[]);
    void initFailpoints() const;
};
} // namespace DB::DM::tests