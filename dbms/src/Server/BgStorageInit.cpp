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
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Server/BgStorageInit.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>

#include <thread>

namespace DB
{
void BgStorageInitHolder::waitUntilFinish()
{
    if (need_join)
    {
        LOG_INFO(Logger::get(), "Wait for storage init thread to join");
        init_thread->join();
        init_thread.reset();
        need_join = false; // join has been done
    }
    // else the job is done by not lazily init
    // or has been detach
}

struct FuncInitStore
{
    const KeyspaceTableID ks_table_id;
    ManageableStoragePtr storage;

    const std::atomic_size_t & terminated;
    std::atomic<Int64> & init_cnt;
    std::atomic<Int64> & err_cnt;
    const size_t total_count;
    // If not null, segments in one table will be restored concurrently
    // Otherwise, segments are restored in serial order
    ThreadPool * restore_segments_thread_pool;
    const LoggerPtr & log;

    void operator()()
    {
        if (terminated.load() != 0)
        {
            LOG_INFO(
                log,
                "cancel init storage, shutting down, keyspace={} table_id={}",
                ks_table_id.first,
                ks_table_id.second);
            return;
        }

        try
        {
            // This will skip the init of storages that do not contain any data. TiFlash now sync the schema and
            // create all tables regardless the table have define TiFlash replica or not, so there may be lots
            // of empty tables in TiFlash.
            // Note that we still need to init stores that contains data (defined by the stable dir of this storage
            // is exist), or the data used size reported to PD is not correct.
            bool init_done = storage->initStoreIfDataDirExist(restore_segments_thread_pool);
            init_cnt += static_cast<Int64>(init_done);
            LOG_INFO(
                log,
                "Storage inited done, keyspace={} table_id={} n_init={} n_err={} n_total={} datatype_fullname_count={}",
                ks_table_id.first,
                ks_table_id.second,
                init_cnt.load(),
                err_cnt.load(),
                total_count,
                DataTypeFactory::instance().getFullNameCacheSize());
        }
        catch (...)
        {
            err_cnt++;
            tryLogCurrentException(
                log,
                fmt::format(
                    "Storage inited fail, keyspace={} table_id={} n_init={} n_err={} n_total={}",
                    ks_table_id.first,
                    ks_table_id.second,
                    init_cnt.load(),
                    err_cnt.load(),
                    total_count));
        }
    }
};

void doInitStores(Context & global_context, const std::atomic_size_t & terminated, const LoggerPtr & log)
{
    const auto storages = global_context.getTMTContext().getStorages().getAllStorage();

    const size_t total_count = storages.size();

    std::atomic<Int64> init_cnt = 0;
    std::atomic<Int64> err_cnt = 0;

    if (global_context.getSettingsRef().init_thread_count_scale > 0)
    {
        size_t num_threads = std::max(4UL, std::thread::hardware_concurrency()) //
            * global_context.getSettingsRef().init_thread_count_scale;
        LOG_INFO(log, "Init stores with thread pool, thread_count={}", num_threads);
        auto init_storages_thread_pool = ThreadPool(num_threads, num_threads / 2, num_threads * 2);
        auto init_storages_wait_group = init_storages_thread_pool.waitGroup();

        auto restore_segments_thread_pool = ThreadPool(num_threads, num_threads / 2, num_threads * 2);

        for (const auto & [ks_tbl_id, storage] : storages)
        {
            auto task = FuncInitStore{
                ks_tbl_id,
                storage,
                terminated,
                init_cnt,
                err_cnt,
                total_count,
                &restore_segments_thread_pool,
                log};

            init_storages_wait_group->schedule(task);
        }

        init_storages_wait_group->wait();
    }
    else
    {
        LOG_INFO(log, "Init stores without thread pool");
        for (const auto & [ks_tbl_id, storage] : storages)
        {
            if (terminated.load() != 0)
            {
                LOG_INFO(log, "cancel init storage, shutting down");
                break;
            }
            // run in serial order
            FuncInitStore{ks_tbl_id, storage, terminated, init_cnt, err_cnt, total_count, nullptr, log}();
        }
    }

    LOG_INFO(
        log,
        "Storage inited finish. total_count={} init_count={} error_count={} terminated={} datatype_fullname_count={}",
        total_count,
        init_cnt,
        err_cnt,
        terminated.load(),
        DataTypeFactory::instance().getFullNameCacheSize());
}

void BgStorageInitHolder::start(
    Context & global_context,
    const std::atomic_size_t & terminated,
    const LoggerPtr & log,
    bool lazily_init_store,
    bool is_s3_enabled)
{
    RUNTIME_CHECK_MSG(
        lazily_init_store || !is_s3_enabled,
        "When S3 enabled, lazily_init_store must be true. lazily_init_store={} s3_enabled={}",
        lazily_init_store,
        is_s3_enabled);

    if (!lazily_init_store)
    {
        LOG_INFO(log, "Not lazily init store.");
        need_join = false;
        doInitStores(global_context, terminated, log);
    }

    LOG_INFO(log, "Lazily init store");
    // apply the inited in another thread to shorten the start time of TiFlash
    if (is_s3_enabled)
    {
        // If s3 enabled, we need to call `waitUntilFinish` before accepting read request later
        init_thread = std::make_unique<std::thread>(
            [&global_context, &terminated, &log] { doInitStores(global_context, terminated, log); });
        need_join = true;
    }
    else
    {
        // Otherwise, just detach the thread
        init_thread = std::make_unique<std::thread>(
            [&global_context, &terminated, &log] { doInitStores(global_context, terminated, log); });
        init_thread->detach();
        need_join = false;
    }
}

} // namespace DB
