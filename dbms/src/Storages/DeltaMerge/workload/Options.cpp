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

#include <Common/FailPoint.h>
#include <Storages/DeltaMerge/workload/Options.h>
#include <Storages/DeltaMerge/workload/Utils.h>
#include <fmt/ranges.h>

#include <boost/program_options.hpp>

namespace DB::DM::tests
{
std::string WorkloadOptions::toString(std::string seperator) const
{
    return fmt::format("max_key_count {}{}", max_key_count, seperator) + //
        fmt::format("write_key_distribution {}{}", write_key_distribution, seperator) + //
        fmt::format("write_count {}{}", write_count, seperator) + //
        fmt::format("write_thread_count {}{}", write_thread_count, seperator) + //
        fmt::format("table {}{}", table, seperator) + //
        fmt::format("pk_type {}{}", pk_type, seperator) + //
        fmt::format("colmuns_count {}{}", columns_count, seperator) + //
        fmt::format("failpoints {}{}", failpoints, seperator) + //
        fmt::format("max_write_per_sec {}{}", max_write_per_sec, seperator) + //
        fmt::format("log_file {}{}", log_file, seperator) + //
        fmt::format("log_level {}{}", log_level, seperator) + //
        fmt::format("verification {}{}", verification, seperator) + //
        fmt::format("verify_round {}{}", verify_round, seperator) + //
        fmt::format("random_kill {}{}", random_kill, seperator) + //
        fmt::format("max_sleep_sec {}{}", max_sleep_sec, seperator) + //
        fmt::format("work_dirs {}{}", work_dirs, seperator) + //
        fmt::format("config_file {}{}", config_file, seperator) + //
        fmt::format("read_thread_count {}{}", read_thread_count, seperator) + //
        fmt::format("read_stream_count {}{}", read_stream_count, seperator) + //
        fmt::format("testing_type {}{}", testing_type, seperator) + //
        fmt::format("log_write_request {}{}", log_write_request, seperator) + //
        fmt::format("ps_run_mode {}{}", magic_enum::enum_name(ps_run_mode), seperator) + //
        fmt::format("bg_thread_count {}{}", bg_thread_count, seperator) + //
        fmt::format("table_id {}{}", table_id, seperator) + //
        fmt::format("table_name {}{}", table_name, seperator) + //
        fmt::format("is_fast_scan {}{}", is_fast_scan, seperator) + //
        fmt::format("enable_read_thread {}{}", enable_read_thread, seperator);
}

std::pair<bool, std::string> WorkloadOptions::parseOptions(int argc, char * argv[])
{
    using boost::program_options::value;
    boost::program_options::options_description desc("Allowed options");

    desc.add_options() //
        ("help", "produce help message") //
        ("max_key_count", value<uint64_t>()->default_value(20000000), "Default is 2000w.") //
        ("write_key_distribution", value<std::string>()->default_value("uniform"), "uniform/normal/incremental") //
        ("write_count", value<uint64_t>()->default_value(5000000), "Default is 500w.") //
        ("write_thread_count", value<uint64_t>()->default_value(4), "") //
        ("max_write_per_sec", value<uint64_t>()->default_value(0), "") //
        //
        ("table", value<std::string>()->default_value("constant"), "constant/random") //
        ("pk_type", value<std::string>()->default_value("tidb_rowid"), "tidb_rowid") //
        ("columns_count", value<uint64_t>()->default_value(0), "0 means random columns count") //
        //
        ("failpoints,F", value<std::vector<std::string>>()->multitoken(), "failpoint(s) to enable: fp1 fp2 fp3...") //
        //
        ("log_file", value<std::string>()->default_value(""), "") //
        ("log_level", value<std::string>()->default_value("information"), "") //
        //
        ("verification", value<bool>()->default_value(true), "") //
        ("verify_round", value<uint64_t>()->default_value(10), "") //
        //
        ("random_kill", value<uint64_t>()->default_value(0), "") //
        ("max_sleep_sec", value<uint64_t>()->default_value(600), "") //
        //
        ("work_dirs",
         value<std::vector<std::string>>()->multitoken()->default_value(
             std::vector<std::string>{"tmp1", "tmp2", "tmp3"},
             "tmp1 tmp2 tmp3"),
         "dir1 dir2 dir3...") //
        ("config_file", value<std::string>()->default_value(""), "Configuation file of DeltaTree") //
        //
        ("read_thread_count", value<uint64_t>()->default_value(1), "") //
        ("read_stream_count", value<uint64_t>()->default_value(4), "") //
        //
        ("testing_type", value<std::string>()->default_value(""), "daily_perf/daily_random/s3_bench") //
        //
        ("log_write_request", value<bool>()->default_value(false), "") //
        //
        ("ps_run_mode",
         value<uint64_t>()->default_value(
             2,
             "possible value: 1(only_v2), 2(only_v3), 3(mix_mode), and note that in mix_mode, the test will run twice, "
             "first round in only_v2 mode and second round in mix_mode")) //
        //
        ("bg_thread_count", value<uint64_t>()->default_value(4), "") //
        //
        ("table_name", value<std::string>()->default_value(""), "") //
        ("table_id", value<int64_t>()->default_value(-1), "") //
        ("is_fast_scan",
         value<bool>()->default_value(false),
         "default is false, means normal mode. When we in fast mode, we should set verification as false") //
        ("enable_read_thread", value<bool>()->default_value(true), "") //
        //
        ("s3_bucket", value<std::string>()->default_value(""), "") //
        ("s3_endpoint", value<std::string>()->default_value(""), "") //
        ("s3_access_key_id", value<std::string>()->default_value(""), "") //
        ("s3_secret_access_key", value<std::string>()->default_value(""), "") //
        ("s3_root", value<std::string>()->default_value(""), "") //
        ("s3_put_concurrency", value<UInt64>()->default_value(16), "") //
        ("s3_get_concurrency", value<UInt64>()->default_value(16), "") //
        ("s3_put_count_per_thread", value<UInt64>()->default_value(16), "") //
        ("s3_get_count_per_thread", value<UInt64>()->default_value(16), "") //
        ("s3_temp_dir", value<std::string>()->default_value("./s3_tmp"), "");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help") > 0)
    {
        std::stringstream ss;
        ss << desc;
        return {false, ss.str()};
    }

    max_key_count = vm["max_key_count"].as<uint64_t>();
    write_key_distribution = vm["write_key_distribution"].as<std::string>();
    write_count = vm["write_count"].as<uint64_t>();
    write_thread_count = vm["write_thread_count"].as<uint64_t>();
    max_write_per_sec = vm["max_write_per_sec"].as<uint64_t>();

    table = vm["table"].as<std::string>();
    pk_type = vm["pk_type"].as<std::string>();
    if (pk_type != "tidb_rowid")
    {
        return {false, fmt::format("pk_type must be tidb_rowid.")};
    }
    columns_count = vm["columns_count"].as<uint64_t>();

    if (vm.count("failpoints"))
    {
        failpoints = vm["failpoints"].as<std::vector<std::string>>();
    }

    log_file = vm["log_file"].as<std::string>();
    log_level = vm["log_level"].as<std::string>();

    verification = vm["verification"].as<bool>();
    verify_round = vm["verify_round"].as<uint64_t>();

    random_kill = vm["random_kill"].as<uint64_t>();
    max_sleep_sec = vm["max_sleep_sec"].as<uint64_t>();

    if (vm.count("work_dirs"))
    {
        work_dirs = vm["work_dirs"].as<std::vector<std::string>>();
    }
    config_file = vm["config_file"].as<std::string>();

    // Randomly kill could cause DeltaMergeStore loss some data, so disallow verification and random_kill both enable.
    if (verification && random_kill > 0)
    {
        return {
            false,
            fmt::format(
                "Disallow verification({}) and randomly kill({}) are enabled simultaneously.",
                verification,
                random_kill)};
    }

    if (log_file.empty())
    {
        log_file = fmt::format("{}/dt_workload_{}.log", work_dirs[0], localTime());
    }

    read_thread_count = vm["read_thread_count"].as<uint64_t>();
    read_stream_count = vm["read_stream_count"].as<uint64_t>();

    testing_type = vm["testing_type"].as<std::string>();
    log_write_request = vm["log_write_request"].as<bool>();

    auto opt_ps_run_mode = magic_enum::enum_cast<PageStorageRunMode>(vm["ps_run_mode"].as<uint64_t>());
    RUNTIME_CHECK_MSG(opt_ps_run_mode.has_value(), "Invalid ps_run_mode={}", vm["ps_run_mode"].as<uint64_t>());
    ps_run_mode = *opt_ps_run_mode;

    bg_thread_count = vm["bg_thread_count"].as<uint64_t>();

    table_id = vm["table_id"].as<int64_t>();
    table_name = vm["table_name"].as<std::string>();
    is_fast_scan = vm["is_fast_scan"].as<bool>();

    if (is_fast_scan && verification)
    {
        return {false, fmt::format("When in_fast_scan, we should set verification as false")};
    }

    enable_read_thread = vm["enable_read_thread"].as<bool>();

    s3_bucket = vm["s3_bucket"].as<String>();
    s3_endpoint = vm["s3_endpoint"].as<String>();
    s3_access_key_id = vm["s3_access_key_id"].as<String>();
    s3_secret_access_key = vm["s3_secret_access_key"].as<String>();
    s3_root = vm["s3_root"].as<String>();
    s3_put_concurrency = vm["s3_put_concurrency"].as<UInt64>();
    s3_get_concurrency = vm["s3_get_concurrency"].as<UInt64>();
    s3_put_count_per_thread = vm["s3_put_count_per_thread"].as<UInt64>();
    s3_get_count_per_thread = vm["s3_get_count_per_thread"].as<UInt64>();
    if (vm.count("s3_temp_dir") > 0)
    {
        s3_temp_dir = vm["s3_temp_dir"].as<String>();
    }

    return {true, toString()};
}

void WorkloadOptions::initFailpoints() const
{
#ifdef FIU_ENABLE
    fiu_init(0);
    for (const auto & fp : failpoints)
    {
        DB::FailPointHelper::enableFailPoint(fp);
    }
#endif
}

} // namespace DB::DM::tests
