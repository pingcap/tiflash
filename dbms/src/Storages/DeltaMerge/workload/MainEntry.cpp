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

#include <Common/Config/TOMLConfiguration.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/UniThreadPool.h>
#include <IO/IOThreadPools.h>
#include <IO/UseSSL.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Server/ServerInfo.h>
#include <Storages/DeltaMerge/ReadThread/ColumnSharingCache.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/workload/DTWorkload.h>
#include <Storages/DeltaMerge/workload/Handle.h>
#include <Storages/DeltaMerge/workload/KeyGenerator.h>
#include <Storages/DeltaMerge/workload/Options.h>
#include <Storages/DeltaMerge/workload/Utils.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/PathPool.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/logger_useful.h>
#include <cpptoml.h>
#include <signal.h>
#include <sys/wait.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>

using namespace DB;
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

// By default init global thread pool by hardware_concurrency
// Later we will adjust it by `adjustThreadPoolSize`
void initThreadPool()
{
    size_t default_num_threads = std::max(4UL, 2 * std::thread::hardware_concurrency());
    GlobalThreadPool::initialize(
        /*max_threads*/ default_num_threads * 20,
        /*max_free_threads*/ default_num_threads,
        /*queue_size*/ default_num_threads * 10);
    S3FileCachePool::initialize(
        /*max_threads*/ default_num_threads,
        /*max_free_threads*/ default_num_threads / 2,
        /*queue_size*/ default_num_threads * 2);
    DataStoreS3Pool::initialize(
        /*max_threads*/ default_num_threads,
        /*max_free_threads*/ default_num_threads / 2,
        /*queue_size*/ default_num_threads * 2);
    RNRemoteReadTaskPool::initialize(
        /*max_threads*/ default_num_threads,
        /*max_free_threads*/ default_num_threads / 2,
        /*queue_size*/ default_num_threads * 2);
    RNPagePreparerPool::initialize(
        /*max_threads*/ default_num_threads,
        /*max_free_threads*/ default_num_threads / 2,
        /*queue_size*/ default_num_threads * 2);
    RNWritePageCachePool::initialize(
        /*max_threads*/ default_num_threads,
        /*max_free_threads*/ default_num_threads / 2,
        /*queue_size*/ default_num_threads * 2);
}

void initReadThread()
{
    DB::ServerInfo server_info;
    DB::DM::SegmentReaderPoolManager::instance().init(
        server_info.cpu_info.logical_cores,
        TiFlashTestEnv::getGlobalContext().getSettingsRef().dt_read_thread_count_scale);
    DB::DM::SegmentReadTaskScheduler::instance();
    DB::DM::DMFileReaderPool::instance();
}

static constexpr StoreID test_store_id = 100000;
DB::Settings createSettings(const WorkloadOptions & opts)
{
    DB::Settings settings;
    if (!opts.config_file.empty())
    {
        auto table = cpptoml::parse_file(opts.config_file);
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
        config->add(new DB::TOMLConfiguration(table), /*shared=*/false); // Take ownership of TOMLConfig
        settings.setProfile("default", *config);
    }

    settings.dt_enable_read_thread = opts.enable_read_thread;
    return settings;
}

ContextPtr init(WorkloadOptions & opts)
{
    log_ofs.open(opts.log_file, std::ios_base::out | std::ios_base::app);
    if (!log_ofs)
    {
        throw std::logic_error(fmt::format("WorkloadOptions::init - Open {} ret {}", opts.log_file, strerror(errno)));
    }
    TiFlashTestEnv::setupLogger(opts.log_level, log_ofs);
    opts.initFailpoints();
    DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V5; // metav2 is used forcibly for test.
    // For mixed mode, we need to run the test in ONLY_V2 mode first.
    auto ps_run_mode
        = opts.ps_run_mode == DB::PageStorageRunMode::MIX_MODE ? DB::PageStorageRunMode::ONLY_V2 : opts.ps_run_mode;
    TiFlashTestEnv::initializeGlobalContext(opts.work_dirs, ps_run_mode, opts.bg_thread_count);

    if (!opts.s3_bucket.empty())
    {
        DB::StorageS3Config config = {
            .endpoint = opts.s3_endpoint,
            .bucket = opts.s3_bucket,
            .access_key_id = opts.s3_access_key_id,
            .secret_access_key = opts.s3_secret_access_key,
            .root = opts.s3_root,
        };
        DB::S3::ClientFactory::instance().init(config);
        initThreadPool();
    }

    if (opts.enable_read_thread)
    {
        initReadThread();
    }

    auto settings = createSettings(opts);
    auto context = DB::tests::TiFlashTestEnv::getContext(settings, opts.work_dirs);
    if (!opts.s3_bucket.empty())
    {
        auto & kvstore = context->getTMTContext().getKVStore();
        auto store_meta = kvstore->clonedStoreMeta();
        store_meta.set_id(test_store_id);
        kvstore->setStore(store_meta);
        context->getSharedContextDisagg()->initRemoteDataStore(context->getFileProvider(), /*is_s3_enabled*/ true);
    }
    return context;
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
    auto s = fmt::format(
        "{},{},{},{:.2f},{},{}",
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

void run(WorkloadOptions & opts, ContextPtr context)
{
    auto * log = &Poco::Logger::get("DTWorkload_main");
    LOG_INFO(log, "{}", opts.toString());
    std::vector<Statistics> stats;
    try
    {
        context->initializeGlobalPageIdAllocator();
        // HandleTable is a unordered_map that stores handle->timestamp for data verified.
        auto handle_table = createHandleTable(opts);
        // Table Schema
        auto table_gen = TableGenerator::create(opts);
        auto table_info = table_gen->get(opts.table_id, opts.table_name);
        // In this for loop, destroy DeltaMergeStore gracefully and recreate it.
        auto run_test = [&]() {
            for (uint64_t i = 0; i < opts.verify_round; i++)
            {
                DTWorkload workload(opts, handle_table, table_info, context);
                workload.run(i);
                stats.push_back(workload.getStat());
                LOG_INFO(log, "No.{} Workload {} {}", i, opts.write_key_distribution, stats.back().toStrings());
            }
        };
        run_test();

        if (opts.ps_run_mode == DB::PageStorageRunMode::MIX_MODE)
        {
            // clear statistic in DB::PageStorageRunMode::ONLY_V2
            stats.clear();
            auto & global_context = TiFlashTestEnv::getGlobalContext();
            global_context.setPageStorageRunMode(DB::PageStorageRunMode::MIX_MODE);
            global_context.initializeGlobalStoragePoolIfNeed(global_context.getPathPool());
            run_test();
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

void doRunAndRandomKill(WorkloadOptions & opts, ContextPtr context)
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
        run(opts, context);
        exit(0);
    }
    else
    {
        // Parent process.
        randomKill(opts, pid);
    }
}

void runAndRandomKill(WorkloadOptions & opts, ContextPtr context)
{
    try
    {
        for (uint64_t i = 0; i < opts.random_kill; i++)
        {
            doRunAndRandomKill(opts, context);
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

void dailyPerformanceTest(WorkloadOptions & opts, ContextPtr context)
{
    outputResultHeader();
    std::vector<std::string> workloads{"uniform", "normal", "incremental"};
    for (size_t i = 0; i < workloads.size(); i++)
    {
        opts.write_key_distribution = workloads[i];
        opts.table_id = i;
        opts.table_name = workloads[i];
        ::run(opts, context);
    }
}

void dailyRandomTest(WorkloadOptions & opts, ContextPtr context)
{
    outputResultHeader();
    static std::random_device rd;
    static std::mt19937_64 rand_gen(rd());
    opts.table = "random";
    for (int i = 0; i < 3; i++)
    {
        opts.columns_count = rand_gen() % 40 + 10; // 10~49 columns.
        ::run(opts, context);
    }
}


std::mutex mtx_remote_fnames;
std::vector<std::pair<String, UInt64>> remote_fnames;

void addRemoteFname(const String & remote_fname, UInt64 fsize)
{
    std::lock_guard lock(mtx_remote_fnames);
    remote_fnames.push_back({remote_fname, fsize});
}

std::pair<String, UInt64> getRemoteFname()
{
    static auto gen = DB::DM::tests::KeyGenerator::create(WorkloadOptions{.testing_type = "s3_bench"});
    std::lock_guard lock(mtx_remote_fnames);
    return remote_fnames[gen->get64() % remote_fnames.size()];
}

class S3Stat
{
public:
    void addPutStat(const String & key, double seconds)
    {
        std::lock_guard lock(mtx);
        put_stats.emplace_back(key, seconds);
    }
    void addGetStat(const String & key, double seconds)
    {
        std::lock_guard lock(mtx);
        get_stats.emplace_back(key, seconds);
    }

    std::vector<std::pair<String, double>> getPutStat()
    {
        std::lock_guard lock(mtx);
        return put_stats;
    }
    std::vector<std::pair<String, double>> getGutStat()
    {
        std::lock_guard lock(mtx);
        return get_stats;
    }

    std::pair<String, double> getMaxPutStat()
    {
        std::lock_guard lock(mtx);
        auto itr = std::max_element(put_stats.begin(), put_stats.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        return *itr;
    }

    std::pair<String, double> getMaxGetStat()
    {
        std::lock_guard lock(mtx);
        auto itr = std::max_element(get_stats.begin(), get_stats.end(), [](const auto & a, const auto & b) {
            return a.second < b.second;
        });
        return *itr;
    }

private:
    std::mutex mtx;
    std::vector<std::pair<String, double>> put_stats;
    std::vector<std::pair<String, double>> get_stats;
};

S3Stat s3_stat;

String getThreadId()
{
    std::ostringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}

void createThreadDirectoryIfNotExists(const WorkloadOptions & opts)
{
    auto tid = getThreadId();
    auto dir = fmt::format("{}/{}", opts.s3_temp_dir, tid);
    if (!std::filesystem::exists(dir))
    {
        std::filesystem::create_directories(dir);
    }
}

void genFile(const String & fname, UInt64 fsize, char value)
{
    String data(4096, value);
    std::ofstream ostr(fname, std::ios_base::out | std::ios_base::binary);
    RUNTIME_CHECK(ostr.is_open(), fname, strerror(errno));
    std::string_view s(data);
    for (UInt64 i = 0; i < fsize;)
    {
        auto n = std::min(4096, fsize - i);
        ostr << s.substr(0, n);
        i += n;
    }
}

std::shared_ptr<S3::TiFlashS3Client> getS3Client(const WorkloadOptions & /*opts*/)
{
    return S3::ClientFactory::instance().sharedTiFlashClient();
}

void putRandomObject(const DB::DM::tests::WorkloadOptions & opts)
{
    static thread_local String tid = getThreadId();
    static thread_local UInt64 index = 0;
    static thread_local auto gen = DB::DM::tests::KeyGenerator::create(opts);

    auto remote_fname = fmt::format("{}/{}", tid, index++);
    auto local_fname = fmt::format("{}/{}", opts.s3_temp_dir, remote_fname);
    auto fsize = gen->get64();
    char value = gen->get64() % 256;
    genFile(local_fname, fsize, value);
    auto client = getS3Client(opts);
    Stopwatch sw;
    S3::uploadFile(*client, local_fname, remote_fname);
    addRemoteFname(remote_fname, fsize);
    s3_stat.addPutStat(remote_fname, sw.elapsedSeconds());
    std::filesystem::remove(local_fname);
}

void getRandomObject(const DB::DM::tests::WorkloadOptions & opts)
{
    static thread_local String tid = getThreadId();
    static thread_local UInt64 index = 0;
    auto [remote_fname, fsize] = getRemoteFname();
    auto local_fname = fmt::format("{}/{}/{}", opts.s3_temp_dir, tid, index++);
    auto client = getS3Client(opts);
    Stopwatch sw;
    S3::downloadFileByS3RandomAccessFile(client, local_fname, remote_fname);
    s3_stat.addGetStat(remote_fname, sw.elapsedSeconds());
    auto download_size = std::filesystem::file_size(local_fname);
    std::cout << fmt::format("GetObject {} bytes={} cost={:.3f}s", remote_fname, download_size, sw.elapsedSeconds())
              << std::endl;
    RUNTIME_CHECK(fsize = download_size, remote_fname, download_size, fsize);
    std::filesystem::remove(local_fname);
}

void putRandomObjectLoop(const WorkloadOptions & opts)
{
    createThreadDirectoryIfNotExists(opts);
    for (UInt64 i = 0; i < opts.s3_put_count_per_thread; ++i)
    {
        putRandomObject(opts);
    }
}

void getRandomObjectLoop(const WorkloadOptions & opts)
{
    createThreadDirectoryIfNotExists(opts);
    for (UInt64 i = 0; i < opts.s3_get_count_per_thread; ++i)
    {
        getRandomObject(opts);
    }
}

void benchS3(WorkloadOptions & opts)
{
    //Poco::Environment::set("AWS_EC2_METADATA_DISABLED", "true"); // disable to speedup testing
    TiFlashTestEnv::setupLogger(opts.log_level);

    RUNTIME_CHECK(!opts.s3_bucket.empty());
    RUNTIME_CHECK(!opts.s3_endpoint.empty());
    RUNTIME_CHECK(!opts.s3_root.empty());
    RUNTIME_CHECK(opts.s3_put_concurrency > 0);
    RUNTIME_CHECK(opts.s3_get_concurrency > 0);
    RUNTIME_CHECK(!opts.s3_temp_dir.empty(), opts.s3_temp_dir);

    if (!std::filesystem::exists(opts.s3_temp_dir))
    {
        std::filesystem::create_directories(opts.s3_temp_dir);
    }
    else
    {
        RUNTIME_CHECK(std::filesystem::is_directory(opts.s3_temp_dir), opts.s3_temp_dir);
    }

    DB::StorageS3Config config = {
        .endpoint = opts.s3_endpoint,
        .bucket = opts.s3_bucket,
        .access_key_id = opts.s3_access_key_id,
        .secret_access_key = opts.s3_secret_access_key,
        .root = opts.s3_root,
    };
    std::cout << fmt::format("StorageS3Config: {}", config.toString()) << std::endl;
    DB::S3::ClientFactory::instance().init(config);

    // Threads for GetObject
    S3FileCachePool::initialize(
        /*max_threads*/ opts.s3_get_concurrency,
        /*max_free_threads*/ opts.s3_get_concurrency,
        /*queue_size*/ opts.s3_get_concurrency * 2);

    // Threads For PutObject
    DataStoreS3Pool::initialize(
        /*max_threads*/ opts.s3_put_concurrency,
        /*max_free_threads*/ opts.s3_put_concurrency,
        /*queue_size*/ opts.s3_put_concurrency * 2);

    // make remote_fnames not empty.
    createThreadDirectoryIfNotExists(opts);
    putRandomObjectLoop(opts);

    std::vector<std::future<void>> put_results;
    for (UInt64 i = 0; i < opts.s3_put_concurrency; ++i)
    {
        auto task = std::make_shared<std::packaged_task<void()>>([&]() { putRandomObjectLoop(opts); });
        put_results.push_back(task->get_future());
        DataStoreS3Pool::get().scheduleOrThrowOnError([task]() { (*task)(); });
    }

    std::vector<std::future<void>> get_results;
    for (UInt64 i = 0; i < opts.s3_get_concurrency; ++i)
    {
        auto task = std::make_shared<std::packaged_task<void()>>([&]() { getRandomObjectLoop(opts); });
        get_results.push_back(task->get_future());
        S3FileCachePool::get().scheduleOrThrowOnError([task]() { (*task)(); });
    }
    for (auto & f : put_results)
    {
        f.get();
    }

    for (auto & f : get_results)
    {
        f.get();
    }

    {
        auto [key, seconds] = s3_stat.getMaxPutStat();
        std::cout << fmt::format("max put time: {} => {}", key, seconds) << std::endl;
    }
    {
        auto [key, seconds] = s3_stat.getMaxGetStat();
        std::cout << fmt::format("max get time: {} => {}", key, seconds) << std::endl;
    }
    DB::S3::ClientFactory::instance().shutdown();
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

    if (opts.testing_type == "s3_bench")
    {
        UseSSL ssl_holder;
        try
        {
            benchS3(opts);
        }
        catch (...)
        {
            DB::tryLogCurrentException("exception thrown");
        }
        return 0;
    }

    // Log file is created in the first directory of `opts.work_dirs` by default.
    // So create these work_dirs before logger initialization.
    // Attention: This function will remove directory first if `work_dirs` exists.
    initWorkDirs(opts.work_dirs);
    // need to init logger before creating global context,
    // or the logging in global context won't be output to
    // the log file
    auto context = init(opts);

    if (opts.testing_type == "daily_perf")
    {
        dailyPerformanceTest(opts, context);
    }
    else if (opts.testing_type == "daily_random")
    {
        dailyRandomTest(opts, context);
    }
    else
    {
        if (opts.random_kill <= 0)
        {
            ::run(opts, context);
        }
        else
        {
            // Kill the running DeltaMergeStore could cause data loss, since we don't have raft-log here.
            // Disable random_kill by default.
            runAndRandomKill(opts, context);
        }
    }
    TiFlashTestEnv::shutdown();
    return 0;
}
