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
#include <Storages/Transaction/TMTContext.h>
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

void BgStorageInitHolder::start(Context & global_context, const LoggerPtr & log, bool lazily_init_store, bool is_s3_enabled)
{
    RUNTIME_CHECK_MSG(
        lazily_init_store || !is_s3_enabled,
        "When S3 enabled, lazily_init_store must be true. lazily_init_store={} s3_enabled={}",
        lazily_init_store,
        is_s3_enabled);
    auto do_init_stores = [&global_context, &log] {
        auto storages = global_context.getTMTContext().getStorages().getAllStorage();
        int init_cnt = 0;
        int err_cnt = 0;
        for (auto & [ks_table_id, storage] : storages)
        {
            // This will skip the init of storages that do not contain any data. TiFlash now sync the schema and
            // create all tables regardless the table have define TiFlash replica or not, so there may be lots
            // of empty tables in TiFlash.
            // Note that we still need to init stores that contains data (defined by the stable dir of this storage
            // is exist), or the data used size reported to PD is not correct.
            const auto & [ks_id, table_id] = ks_table_id;
            try
            {
                init_cnt += storage->initStoreIfDataDirExist() ? 1 : 0;
                LOG_INFO(log, "Storage inited done, keyspace_id={} table_id={}", ks_id, table_id);
            }
            catch (...)
            {
                err_cnt++;
                tryLogCurrentException(log, fmt::format("Storage inited fail, keyspace_id={} table_id={}", ks_id, table_id));
            }
        }
        LOG_INFO(
            log,
            "Storage inited finish. [total_count={}] [init_count={}] [error_count={}] [datatype_fullname_count={}]",
            storages.size(),
            init_cnt,
            err_cnt,
            DataTypeFactory::instance().getFullNameCacheSize());
    };

    if (!lazily_init_store)
    {
        LOG_INFO(log, "Not lazily init store.");
        need_join = false;
        do_init_stores();
    }

    LOG_INFO(log, "Lazily init store.");
    // apply the inited in another thread to shorten the start time of TiFlash
    if (is_s3_enabled)
    {
        init_thread = std::make_unique<std::thread>(do_init_stores);
        need_join = true;
    }
    else
    {
        init_thread = std::make_unique<std::thread>(do_init_stores);
        init_thread->detach();
        need_join = false;
    }
}

} // namespace DB
