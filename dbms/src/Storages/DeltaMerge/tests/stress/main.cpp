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

#include <Common/Exception.h>
#include <Storages/DeltaMerge/tests/stress/DMStressProxy.h>
#include <fmt/format.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#include <random>

using StressOptions = DB::DM::tests::StressOptions;

StressOptions parseStressOptions(int argc, char * argv[])
{
    using boost::program_options::value;
    boost::program_options::options_description desc("Allowed options");
    desc.add_options() //
        ("help", "produce help message") //
        ("insert_concurrency", value<UInt32>()->default_value(58), "number of insert thread") //
        ("update_concurrency", value<UInt32>()->default_value(40), "number of update thread") //
        ("delete_concurrency", value<UInt32>()->default_value(1), "number of delete thread") //
        ("write_sleep_us", value<UInt32>()->default_value(10), "sleep microseconds between write operators") //
        ("write_rows_per_block", value<UInt32>()->default_value(8), "number of rows per write(insert or update)") //
        ("read_concurrency", value<UInt32>()->default_value(20), "number of read thread") //
        ("read_sleep_us", value<UInt32>()->default_value(20), "sleep microseconds between read operations") //
        ("gen_total_rows", value<UInt64>()->default_value(100000000), "generate data total rows") //
        ("gen_rows_per_block", value<UInt32>()->default_value(128), "generate data rows per block") //
        ("gen_concurrency", value<UInt32>()->default_value(100), "number of generate thread") //
        ("table_name", value<String>()->default_value("stress2"), "Table name") //
        ("verify", value<bool>()->default_value(true), "Verify") //
        ("verify_sleep_sec", value<UInt32>()->default_value(120), "Verify sleep seconds") //
        ("failpoints,F", value<std::vector<std::string>>(), "failpoint(s) to enable") //
        ("min_restart_sec", value<UInt32>()->default_value(600), "minimum interval to restart tiflash") //
        ("max_restart_sec", value<UInt32>()->default_value(3600), "maximum interval to restart tiflash") //
        ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") > 0)
    {
        std::cout << desc << std::endl;
        exit(-1);
    }

    StressOptions stress_options;
    stress_options.insert_concurrency = options["insert_concurrency"].as<UInt32>();
    stress_options.update_concurrency = options["update_concurrency"].as<UInt32>();
    stress_options.delete_concurrency = options["delete_concurrency"].as<UInt32>();
    stress_options.write_sleep_us = options["write_sleep_us"].as<UInt32>();
    stress_options.write_rows_per_block = options["write_rows_per_block"].as<UInt32>();
    stress_options.read_concurrency = options["read_concurrency"].as<UInt32>();
    stress_options.read_sleep_us = options["read_sleep_us"].as<UInt32>();
    stress_options.gen_total_rows = options["gen_total_rows"].as<UInt64>();
    stress_options.gen_rows_per_block = options["gen_rows_per_block"].as<UInt32>();
    stress_options.gen_concurrency = options["gen_concurrency"].as<UInt32>();
    stress_options.table_name = options["table_name"].as<String>();
    stress_options.verify = options["verify"].as<bool>();
    stress_options.verify_sleep_sec = options["verify_sleep_sec"].as<UInt32>();
    stress_options.min_restart_sec = options["min_restart_sec"].as<UInt32>();
    stress_options.max_restart_sec = options["max_restart_sec"].as<UInt32>();

    if (options.count("failpoints"))
        stress_options.failpoints = options["failpoints"].as<std::vector<std::string>>();

    std::cout << " insert_concurrency: " << stress_options.insert_concurrency
              << " update_concurrency: " << stress_options.update_concurrency
              << " delete_concurrency: " << stress_options.delete_concurrency << " write_sleep_us: " << stress_options.write_sleep_us
              << " write_row_per_block: " << stress_options.write_rows_per_block << " read_concurrency: " << stress_options.read_concurrency
              << " read_sleep_us: " << stress_options.read_sleep_us << " gen_row_count: " << stress_options.gen_total_rows
              << " gen_row_per_block: " << stress_options.gen_rows_per_block << " gen_concurrency: " << stress_options.gen_concurrency
              << " table_name: " << stress_options.table_name << " verify: " << stress_options.verify
              << " verify_sleep_sec: " << stress_options.verify_sleep_sec //
              << fmt::format(" failpoints: [{}]", fmt::join(stress_options.failpoints.begin(), stress_options.failpoints.end(), ","))
              << " min_restart_sec: " << stress_options.min_restart_sec
              << " max_restart_sec: " << stress_options.max_restart_sec
              << std::endl;

    return stress_options;
}

void init()
{
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();
#ifdef FIU_ENABLE
    fiu_init(0);
#endif
}

void runProxy(const StressOptions & opts, Poco::Logger * log)
{
    try
    {
        UInt64 run_count = 0;
        while (true)
        {
            auto start = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            run_count++;
            DB::DM::tests::DMStressProxy store_proxy(opts);
            store_proxy.run();
            auto end = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            LOG_FMT_INFO(log, "run_count: {} start: {} end: {} use time: {}", run_count, start, end, (end - start));
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException("runProxy fail");
    }
    DB::tests::TiFlashTestEnv::shutdown();
}

int main(int argc, char * argv[])
{
    init();
    auto opts = parseStressOptions(argc, argv);
    for (const auto & fp : opts.failpoints)
    {
        DB::FailPointHelper::enableFailPoint(fp);
    }
    auto * log = &Poco::Logger::get("DMStressProxy");
    UInt64 run_count = 0;
    static std::uniform_int_distribution<unsigned> dist(opts.min_restart_sec, opts.max_restart_sec);
    std::default_random_engine generator;
    generator.seed(::time(nullptr));
    for (;;)
    {
        run_count++;
        LOG_FMT_INFO(log, "main loop run count: {}", run_count);
        auto fpid = fork();
        if (fpid < 0)
        {
            LOG_FMT_INFO(log, "fork error in run count {}", run_count);
        }
        else if (fpid == 0)
        {
            try
            {
                runProxy(opts, log);
                exit(0);
            }
            catch (...)
            {
                DB::tryLogCurrentException("runProxy fail");
            }
        }
        else
        {
            try
            {
                sleep(dist(generator));
                // sleep(300);
                kill(fpid, SIGKILL);
                int status;
                wait(&status);
                if (WIFEXITED(status))
                {
                    LOG_FMT_INFO(log, "child pid {} exit normally", fpid);
                    break;
                }
            }
            catch (...)
            {
                DB::tryLogCurrentException("runProxy fail");
            }
        }
    }
    return 0;
}
