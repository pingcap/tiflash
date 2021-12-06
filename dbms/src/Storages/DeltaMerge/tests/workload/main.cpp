#include <Storages/DeltaMerge/tests/workload/DTWorkload.h>
#include <Storages/DeltaMerge/tests/workload/Handle.h>
#include <Storages/DeltaMerge/tests/workload/Options.h>
#include <Storages/DeltaMerge/tests/workload/Utils.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <fstream>
#include <random>

using namespace DB::tests;
using namespace DB::DM::tests;

void init(WorkloadOptions & opts)
{
    TiFlashTestEnv::initializeGlobalContext(opts.work_dirs);
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

void removeData(Poco::Logger * log, const std::vector<std::string> & data_dirs)
{
    for (const auto & dir : data_dirs)
    {
        auto cmd = fmt::format("rm -rf {}", dir);
        LOG_ERROR(log, cmd);
        system(cmd.c_str());
    }
}

void print(Poco::Logger * log, uint64_t i, const DTWorkload::Statistics & stat)
{
    auto v = stat.toStrings(i);
    for (const auto & s : v)
    {
        std::cerr << s << std::endl;
        LOG_INFO(log, s);
    }
}

std::shared_ptr<SharedHandleTable> createHandleTable(WorkloadOptions & opts)
{
    return opts.verification ? std::make_unique<SharedHandleTable>() : nullptr;
}

void run(WorkloadOptions & opts)
{
    init(opts);
    auto * log = &Poco::Logger::get("DTWorkload_main");
    auto data_dirs = DB::tests::TiFlashTestEnv::getGlobalContext().getPathPool().listPaths();
    try
    {
        // HandleTable is a unordered_map that stores handle->timestamp for data verified.
        auto handle_table = createHandleTable(opts);
        // In this for loop, destory DeltaMergeStore gracefully and recreate it.
        for (uint64_t i = 0; i < opts.verify_round; i++)
        {
            DTWorkload workload(opts, handle_table);
            workload.run(i);
            auto & stat = workload.getStat();
            print(log, i, stat);
        }
        shutdown();
        removeData(log, data_dirs);
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(log, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknow Exception");
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