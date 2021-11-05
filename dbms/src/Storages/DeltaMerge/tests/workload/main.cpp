#include <Storages/DeltaMerge/tests/workload/DTWorkload.h>
#include <Storages/DeltaMerge/tests/workload/Options.h>
#include <Storages/DeltaMerge/tests/workload/Utils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <fstream>
#include <random>

using namespace DB::tests;
using namespace DB::DM::tests;

void init(WorkloadOptions & opts)
{
    TiFlashTestEnv::initializeGlobalContext();
    static std::ofstream log_ofs(opts.log_file, std::ios_base::out | std::ios_base::app);
    if (!log_ofs)
    {
        throw std::logic_error(fmt::format("WorkloadOptions::init - Open {} ret {}", opts.log_file, strerror(errno)));
    }
    TiFlashTestEnv::setupLogger(opts.log_level, log_ofs);
    opts.initFailpoints();
}

void shutdown()
{
    TiFlashTestEnv::shutdown();
}

void print(uint64_t i, const DTWorkload::Statistics & stat)
{
    std::cerr << fmt::format("[{}]{}\n", i, localTime());
    std::cerr << fmt::format("init_store_sec {} init_handle_table_sec {}\n", stat.init_store_sec, stat.init_handle_table_sec);
    std::cerr << fmt::format("total_read_count {} total_read_sec {}\n", stat.total_read_count, stat.total_read_sec);
    for (size_t k = 0; k < stat.write_stats.size(); k++)
    {
        std::cerr << fmt::format("Thread[{}] {}\n", k, stat.write_stats[k].toString());
    }
    std::cerr << fmt::format("verify_count {} verify_sec {}\n", stat.verify_count, stat.verify_sec);
}

void run(WorkloadOptions & opts)
{
    try
    {
        init(opts);
        // In this for loop, destory DeltaMergeStore gracefully and recreate it.
        for (uint64_t i = 0; i < opts.verify_round; i++)
        {
            DTWorkload workload(opts);
            workload.run(i);
            auto stat = workload.getStat();
            print(i, stat);
        }
        shutdown();
    }
    catch (const DB::Exception & e)
    {
        std::cerr << localTime() << e.message() << std::endl;
    }
    catch (const std::exception & e)
    {
        std::cerr << localTime() << " " << e.what() << std::endl;
    }
    catch (...)
    {
        std::cerr << localTime() << " "
                  << "Unknow Exception" << std::endl;
    }
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
    wait(&status);
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

int main(int argc, char ** argv)
{
    WorkloadOptions opts;
    auto [ok, msg] = opts.parseOptions(argc, argv);
    std::cerr << msg << std::endl;
    if (!ok)
    {
        return -1;
    }

    if (opts.random_kill <= 0)
    {
        run(opts);
    }
    else
    {
        // Kill the running DeltaMergeStore could cause data loss, since we don't have raft-log here.
        // Disable random_kill by default.
        runAndRandomKill(opts);
    }
    return 0;
}