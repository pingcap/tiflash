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


#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTracker.h>
#include <Common/UnifiedLogFormatter.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/Formatter.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Storages/Page/workload/PSStressEnv.h>
#include <Storages/Page/workload/PSWorkload.h>
#include <signal.h>

#include <boost/program_options.hpp>

namespace DB::PS::tests
{
LoggerPtr StressEnv::buildLogger(bool enable_color)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::Formatter> formatter;
    if (enable_color)
        formatter = new DB::UnifiedLogFormatter<true>();
    else
        formatter = new DB::UnifiedLogFormatter<false>();
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel("trace");
    return Logger::get();
}

StressEnv StressEnv::parse(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using po::value;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "produce help message") //
        ("write_concurrency,W", value<UInt32>()->default_value(4), "number of write threads") //
        ("read_concurrency,R", value<UInt32>()->default_value(16), "number of read threads") //
        ("dropdata", value<bool>()->default_value(true), "drop data before running") //
        ("init_pages", value<bool>()->default_value(false), "init pages if not exist before running") //
        ("timeout,T", value<UInt32>()->default_value(600), "maximum run time (seconds). 0 means run infinitely") //
        ("writer_slots", value<UInt32>()->default_value(4), "number of PageStorage writer slots (for V2)") //
        ("read_delay_ms", value<UInt32>()->default_value(0), "millionseconds of read delay") //
        ("avg_page_size",
         value<UInt32>()->default_value(2 * 1024 * 1024),
         "avg size for each page(bytes). 2 MiB by default") //
        ("paths,P", value<std::vector<std::string>>(), "store path(s)") //
        ("failpoints", value<std::vector<std::string>>(), "failpoint(s) to enable") //
        ("gc_interval", value<UInt32>()->default_value(30), "GC interval(seconds). 0 means no gc") //
        ("status_interval",
         value<UInt32>()->default_value(5),
         "Status statistics interval(seconds). 0 means no statistics") //
        ("situation_mask,M", value<UInt64>()->default_value(0), "Run special tests sequentially, example -M 2") //
        ("verify", value<bool>()->default_value(true), "Run special tests sequentially with verify.") //
        ("running_ps_version,V",
         value<UInt16>()->default_value(3),
         "Select a version of PageStorage. 2 or 3 can used") //
        ("color", value<bool>()->default_value(true), "enable color output");

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);
    po::notify(options);

    if (options.count("help") > 0)
    {
        std::cerr << desc << std::endl;
        std::cerr << PageWorkloadFactory::getInstance().toDebugStirng() << std::endl;
        exit(0);
    }

    StressEnv opt;
    opt.num_writers = options["write_concurrency"].as<UInt32>();
    opt.num_readers = options["read_concurrency"].as<UInt32>();
    opt.init_pages = options["init_pages"].as<bool>();
    opt.dropdata = options["dropdata"].as<bool>();
    opt.timeout_s = options["timeout"].as<UInt32>();
    opt.read_delay_ms = options["read_delay_ms"].as<UInt32>();
    opt.num_writer_slots = options["writer_slots"].as<UInt32>();
    opt.avg_page_size = options["avg_page_size"].as<UInt32>();
    opt.gc_interval_s = options["gc_interval"].as<UInt32>();
    opt.status_interval = options["status_interval"].as<UInt32>();
    opt.situation_mask = options["situation_mask"].as<UInt64>();
    opt.verify = options["verify"].as<bool>();
    opt.running_ps_version = options["running_ps_version"].as<UInt16>();
    opt.logger = buildLogger(options["color"].as<bool>());

    if (opt.running_ps_version != 2 && opt.running_ps_version != 3)
    {
        std::cerr << "Invalid running_ps_version, this arg should be 2 or 3." << std::endl;
        std::cerr << desc << std::endl;
        exit(0);
    }

    RUNTIME_CHECK(opt.avg_page_size > 0);

    if (options.count("paths"))
        opt.paths = options["paths"].as<std::vector<std::string>>();
    else
        opt.paths = {"./stress"};

    if (options.count("failpoints"))
        opt.failpoints = options["failpoints"].as<std::vector<std::string>>();
    return opt;
}

void setupSignal()
{
    signal(SIGINT, [](int /*signal*/) {
        LOG_INFO(Logger::get(), "Receive finish signal. Wait for the threads finish");
        StressEnvStatus::getInstance().setStat(StressEnvStat::STATUS_INTERRUPT);
        PageWorkloadFactory::getInstance().stopWorkload();
    });
}

void StressEnv::setup()
{
    CurrentMemoryTracker::disableThreshold();
#ifdef FIU_ENABLE
    fiu_init(0);
#endif


    for (const auto & fp : failpoints)
    {
        DB::FailPointHelper::enableFailPoint(fp);
    }

    // drop dir if exists
    bool all_directories_not_exist = true;
    for (const auto & path : paths)
    {
        if (Poco::File file(path); file.exists())
        {
            all_directories_not_exist = false;
            if (dropdata)
            {
                file.remove(true);
            }
        }
    }

    if (dropdata)
        LOG_INFO(logger, "All pages have been drop.");

    if (dropdata || all_directories_not_exist)
        init_pages = true;
    setupSignal();
}
} // namespace DB::PS::tests
