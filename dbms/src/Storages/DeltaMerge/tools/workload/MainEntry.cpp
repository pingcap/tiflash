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
#include <Common/Logger.h>
#include <Storages/DeltaMerge/tools/workload/DTWorkload.h>
#include <Storages/DeltaMerge/tools/workload/Handle.h>
#include <Storages/DeltaMerge/tools/workload/Options.h>
#include <Storages/DeltaMerge/tools/workload/Utils.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/logger_useful.h>
#include <signal.h>
#include <sys/wait.h>

#include <fstream>
#include <random>

using namespace DB::tests;
using namespace DB::DM::tests;

std::ofstream log_ofs;

void initWorkDirs(const std::vector<std::string> & dirs)
{
    for (const auto & dir : dirs)
    {
        Poco::File d(dir);
        if (d.exists())
        {
            d.remove(true);
        }
        d.createDirectories();
    }
}

void init(WorkloadOptions & opts)
{
    log_ofs.open(opts.log_file, std::ios_base::out | std::ios_base::app);
    if (!log_ofs)
    {
        throw std::logic_error(fmt::format("WorkloadOptions::init - Open {} ret {}", opts.log_file, strerror(errno)));
    }
    TiFlashTestEnv::setupLogger(opts.log_level, log_ofs);
    opts.initFailpoints();
}

void outputResultHeader()
{
    std::cout << "Date,Table Schema,Workload,Init Seconds,Write Speed(rows count),Read Speed(rows count)" << std::endl;
}

uint64_t average(const std::vector<uint64_t> & v)
{
    if (v.empty())
    {
        return 0;
    }
    size_t ignore_element_count = v.size() * 0.1;
    auto begin = v.begin() + ignore_element_count; // Ignore small elements.
    auto end = v.end() - ignore_element_count; // Ignore large elements.
    auto count = end - begin;
    return std::accumulate(begin, end, 0ul) / count;
}

void outputResult(Poco::Logger * log, const std::vector<Statistics> & stats, WorkloadOptions & opts)
{
    if (stats.empty())
    {
        return;
    }

    uint64_t max_init_ms = 0;
    std::vector<uint64_t> write_per_seconds, read_per_seconds;
    for_each(stats.begin(), stats.end(), [&](const Statistics & stat) {
        max_init_ms = std::max(max_init_ms, stat.initMS());
        write_per_seconds.push_back(stat.writePerSecond());
        read_per_seconds.push_back(stat.readPerSecond());
    });

    std::sort(write_per_seconds.begin(), write_per_seconds.end());
    std::sort(read_per_seconds.begin(), read_per_seconds.end());

    auto avg_write_per_second = average(write_per_seconds);
    auto avg_read_per_second = average(read_per_seconds);

    // Date, Table Schema, Workload, Init Seconds, Write Speed(rows count), Read Speed(rows count)
    auto s = fmt::format("{},{},{},{:.2f},{},{}",
                         localDate(),
                         opts.table,
                         opts.write_key_distribution,
                         max_init_ms / 1000.0,
                         avg_write_per_second,
                         avg_read_per_second);
    LOG_INFO(log, s);
    std::cout << s << std::endl;
}

std::shared_ptr<SharedHandleTable> createHandleTable(WorkloadOptions & opts)
{
    return opts.verification ? std::make_unique<SharedHandleTable>(opts.max_key_count) : nullptr;
}

void run(WorkloadOptions & opts)
{
    auto * log = &Poco::Logger::get("DTWorkload_main");
    LOG_FMT_INFO(log, "{}", opts.toString());
    std::vector<Statistics> stats;
    try
    {
        // HandleTable is a unordered_map that stores handle->timestamp for data verified.
        auto handle_table = createHandleTable(opts);
        // Table Schema
        auto table_gen = TableGenerator::create(opts);
        auto table_info = table_gen->get(opts.table_id, opts.table_name);
        // In this for loop, destory DeltaMergeStore gracefully and recreate it.
        for (uint64_t i = 0; i < opts.verify_round; i++)
        {
            DTWorkload workload(opts, handle_table, table_info);
            workload.run(i);
            stats.push_back(workload.getStat());
            LOG_FMT_INFO(log, "No.{} Workload {} {}", i, opts.write_key_distribution, stats.back().toStrings());
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException("exception thrown");
        std::abort(); // Finish testing if some error happened.
    }

    outputResult(log, stats, opts);
}

void randomKill(WorkloadOptions & opts, pid_t pid)
{
    static std::random_device rd;
    static std::mt19937_64 rand_gen(rd());
    auto sleep_sec = rand_gen() % opts.max_sleep_sec + 1;
    std::cerr << fmt::format("{} sleep seconds {}", localTime(), sleep_sec) << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));

    int ret = kill(pid, SIGKILL);
    if (ret < 0)
    {
        std::cerr << fmt::format("{} kill pid {} ret {}.", localTime(), pid, strerror(errno)) << std::endl;
    }
    else
    {
        std::cerr << fmt::format("{} kill pid {} succ.", localTime(), pid) << std::endl;
    }
    int status = 0;
    ::wait(&status);
}

void doRunAndRandomKill(WorkloadOptions & opts)
{
    auto pid = fork();
    if (pid < 0)
    {
        throw std::runtime_error(fmt::format("fork ret {}", strerror(errno)));
    }

    // Assume the execution time of 'run' is greater than the random wait time of 'randomKill',
    // so 'randomKill' can kill the child process.
    if (pid == 0)
    {
        // Child process.
        run(opts);
        exit(0);
    }
    else
    {
        // Parent process.
        randomKill(opts, pid);
    }
}

void runAndRandomKill(WorkloadOptions & opts)
{
    try
    {
        for (uint64_t i = 0; i < opts.random_kill; i++)
        {
            doRunAndRandomKill(opts);
        }
    }
    catch (const DB::Exception & e)
    {
        std::cerr << localTime() << " " << e.message() << std::endl;
    }
    catch (const std::exception & e)
    {
        std::cerr << localTime() << " " << e.what() << std::endl;
    }
    catch (...)
    {
        std::cerr << localTime() << " "
                  << "Unknow exception" << std::endl;
    }
}

void dailyPerformanceTest(WorkloadOptions & opts)
{
    outputResultHeader();
    std::vector<std::string> workloads{"uniform", "normal", "incremental"};
    for (size_t i = 0; i < workloads.size(); i++)
    {
        opts.write_key_distribution = workloads[i];
        opts.table_id = i;
        opts.table_name = workloads[i];
        ::run(opts);
    }
}

void dailyRandomTest(WorkloadOptions & opts)
{
    outputResultHeader();
    static std::random_device rd;
    static std::mt19937_64 rand_gen(rd());
    opts.table = "random";
    for (int i = 0; i < 3; i++)
    {
        opts.columns_count = rand_gen() % 40 + 10; // 10~49 columns.
        ::run(opts);
    }
}

int DTWorkload::mainEntry(int argc, char ** argv)
{
    WorkloadOptions opts;
    auto [ok, msg] = opts.parseOptions(argc, argv);
    if (!ok)
    {
        std::cerr << msg << std::endl;
        return -1;
    }

    // Log file is created in the first directory of `opts.work_dirs` by default.
    // So create these work_dirs before logger initialization.
    // Attention: This function will remove directory first if `work_dirs` exists.
    initWorkDirs(opts.work_dirs);
    // need to init logger before creating global context,
    // or the logging in global context won't be output to
    // the log file
    init(opts);
    TiFlashTestEnv::initializeGlobalContext(opts.work_dirs, opts.enable_ps_v3);

    if (opts.testing_type == "daily_perf")
    {
        dailyPerformanceTest(opts);
    }
    else if (opts.testing_type == "daily_random")
    {
        dailyRandomTest(opts);
    }
    else
    {
        if (opts.random_kill <= 0)
        {
            ::run(opts);
        }
        else
        {
            // Kill the running DeltaMergeStore could cause data loss, since we don't have raft-log here.
            // Disable random_kill by default.
            runAndRandomKill(opts);
        }
    }

    TiFlashTestEnv::shutdown();
    return 0;
}
