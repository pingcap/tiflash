#include <Common/FailPoint.h>
#include <Storages/DeltaMerge/tests/workload/Options.h>
#include <Storages/DeltaMerge/tests/workload/Utils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/ranges.h>

#include <boost/program_options.hpp>

namespace DB::DM::tests
{
std::string WorkloadOptions::toString() const
{
    return fmt::format("max_key_count {}\n", max_key_count) + //
        fmt::format("write_key_distribution {}\n", write_key_distribution) + //
        fmt::format("write_count {}\n", write_count) + //
        fmt::format("write_thread_count {}\n", write_thread_count) + //
        fmt::format("table {}\n", table) + //
        fmt::format("pk_type {}\n", pk_type) + //
        fmt::format("colmuns_count {}\n", columns_count) + //
        fmt::format("failpoints {}\n", failpoints) + //
        fmt::format("max_write_per_sec {}\n", max_write_per_sec) + //
        fmt::format("log_file {}\n", log_file) + //
        fmt::format("log_level {}\n", log_level) + //
        fmt::format("verification {}\n", verification) + //
        fmt::format("verify_round {}\n", verify_round) + //
        fmt::format("random_kill {}\n", random_kill) + //
        fmt::format("max_sleep_sec {}\n", max_sleep_sec) + //
        fmt::format("work_dirs {}\n", work_dirs) + //
        fmt::format("config_file {}\n", config_file) + //
        fmt::format("read_thread_count {}\n", read_thread_count) + //
        fmt::format("read_stream_count {}\n", read_stream_count);
}

std::pair<bool, std::string> WorkloadOptions::parseOptions(int argc, char * argv[])
{
    using boost::program_options::value;
    boost::program_options::options_description desc("Allowed options");

    desc.add_options() //
        ("help", "produce help message") //
        ("max_key_count", value<uint64_t>()->default_value(1000000), "") //
        ("write_key_distribution", value<std::string>()->default_value("uniform"), "uniform/normal/incremental") //
        ("write_count", value<uint64_t>()->default_value(1000000), "") //
        ("write_thread_count", value<uint64_t>()->default_value(1), "") //
        ("max_write_per_sec", value<uint64_t>()->default_value(0), "") //
        //
        ("table", value<std::string>()->default_value("constant"), "constant/random") //
        ("pk_type", value<std::string>()->default_value(""), "tidb_rowid/pk_is_handle64") //
        ("columns_count", value<uint64_t>()->default_value(0), "0 means random columns count") //
        //
        ("failpoints,F", value<std::vector<std::string>>()->multitoken(), "failpoint(s) to enable: fp1 fp2 fp3...") //
        //
        ("log_file", value<std::string>()->default_value(""), "") //
        ("log_level", value<std::string>()->default_value("debug"), "") //
        //
        ("verification", value<bool>()->default_value(true), "") //
        ("verify_round", value<uint64_t>()->default_value(10), "") //
        //
        ("random_kill", value<uint64_t>()->default_value(0), "") //
        ("max_sleep_sec", value<uint64_t>()->default_value(600), "") //
        //
        ("work_dirs", value<std::vector<std::string>>()->multitoken()->default_value(std::vector<std::string>{"tmp1", "tmp2", "tmp3"}, "tmp1 tmp2 tmp3"), "dir1 dir2 dir3...") //
        ("config_file", value<std::string>()->default_value(""), "Configuation file of DeltaTree") //
        //
        ("read_thread_count", value<uint64_t>()->default_value(1), "") //
        ("read_stream_count", value<uint64_t>()->default_value(4), "") //
        ;

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
        return {false, fmt::format("Disallow verification({}) and randomly kill({}) are enabled simultaneously.", verification, random_kill)};
    }

    if (log_file.empty())
    {
        log_file = fmt::format("{}/dt_workload_{}.log", work_dirs[0], localTime());
    }

    read_thread_count = vm["read_thread_count"].as<uint64_t>();
    read_stream_count = vm["read_stream_count"].as<uint64_t>();

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