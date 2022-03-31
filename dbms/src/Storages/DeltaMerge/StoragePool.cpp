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

#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/PathPool.h>
#include <fmt/format.h>

namespace DB
{
namespace FailPoints
{
extern const char force_set_dtfile_exist_when_acquire_id[];
} // namespace FailPoints
namespace DM
{
enum class StorageType
{
    Log = 1,
    Data = 2,
    Meta = 3,
};

PageStorage::Config extractConfig(const Settings & settings, StorageType subtype)
{
#define SET_CONFIG(NAME)                                                            \
    config.num_write_slots = settings.dt_storage_pool_##NAME##_write_slots;         \
    config.gc_min_files = settings.dt_storage_pool_##NAME##_gc_min_file_num;        \
    config.gc_min_bytes = settings.dt_storage_pool_##NAME##_gc_min_bytes;           \
    config.gc_min_legacy_num = settings.dt_storage_pool_##NAME##_gc_min_legacy_num; \
    config.gc_max_valid_rate = settings.dt_storage_pool_##NAME##_gc_max_valid_rate;

    PageStorage::Config config = getConfigFromSettings(settings);


    switch (subtype)
    {
    case StorageType::Log:
        SET_CONFIG(log);
        break;
    case StorageType::Data:
        SET_CONFIG(data);
        break;
    case StorageType::Meta:
        SET_CONFIG(meta);
        break;
    default:
        throw Exception("Unknown subtype in extractConfig: " + DB::toString(static_cast<Int32>(subtype)));
    }
#undef SET_CONFIG

    return config;
}

template <class T>
static bool doStoragePoolGC(const Context & global_context, const Settings & settings, const T & storage_pool)
{
    bool done_anything = false;
    auto write_limiter = global_context.getWriteLimiter();
    auto read_limiter = global_context.getReadLimiter();
    auto config = extractConfig(settings, StorageType::Meta);
    storage_pool.meta()->reloadSettings(config);
    done_anything |= storage_pool.meta()->gc(/*not_skip*/ false, write_limiter, read_limiter);

    config = extractConfig(settings, StorageType::Data);
    storage_pool.data()->reloadSettings(config);
    done_anything |= storage_pool.data()->gc(/*not_skip*/ false, write_limiter, read_limiter);

    config = extractConfig(settings, StorageType::Log);
    storage_pool.log()->reloadSettings(config);
    done_anything |= storage_pool.log()->gc(/*not_skip*/ false, write_limiter, read_limiter);

    return done_anything;
}

GlobalStoragePool::GlobalStoragePool(const PathPool & path_pool, Context & global_ctx, const Settings & settings)
    : // The iops and bandwidth in log_storage are relatively high, use multi-disks if possible
    log_storage(PageStorage::create("__global__.log",
                                    path_pool.getPSDiskDelegatorGlobalMulti("log"),
                                    extractConfig(settings, StorageType::Log),
                                    global_ctx.getFileProvider(),
                                    true))
    ,
    // The iops in data_storage is low, only use the first disk for storing data
    data_storage(PageStorage::create("__global__.data",
                                     path_pool.getPSDiskDelegatorGlobalSingle("data"),
                                     extractConfig(settings, StorageType::Data),
                                     global_ctx.getFileProvider(),
                                     true))
    ,
    // The iops in meta_storage is relatively high, use multi-disks if possible
    meta_storage(PageStorage::create("__global__.meta",
                                     path_pool.getPSDiskDelegatorGlobalMulti("meta"),
                                     extractConfig(settings, StorageType::Meta),
                                     global_ctx.getFileProvider(),
                                     true))
    , global_context(global_ctx)
{}

void GlobalStoragePool::restore()
{
    log_storage->restore();
    data_storage->restore();
    meta_storage->restore();

    gc_handle = global_context.getBackgroundPool().addTask(
        [this] {
            return this->gc(global_context.getSettingsRef());
        },
        false);
}

GlobalStoragePool::~GlobalStoragePool()
{
    if (gc_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        gc_handle = nullptr;
    }
}

bool GlobalStoragePool::gc(const Settings & settings, const Seconds & try_gc_period)
{
    {
        std::lock_guard lock(mutex);

        Timepoint now = Clock::now();
        if (now < (last_try_gc_time.load() + try_gc_period))
            return false;

        last_try_gc_time = now;
    }

    return doStoragePoolGC(global_context, settings, *this);
}


StoragePool::StoragePool(const String & name, NamespaceId ns_id_, StoragePathPool & path_pool, Context & global_ctx, const Settings & settings)
    : owned_storage(true)
    , ns_id(ns_id_)
    ,
    // The iops and bandwidth in log_storage are relatively high, use multi-disks if possible
    log_storage(PageStorage::create(name + ".log",
                                    path_pool.getPSDiskDelegatorMulti("log"),
                                    extractConfig(settings, StorageType::Log),
                                    global_ctx.getFileProvider()))
    ,
    // The iops in data_storage is low, only use the first disk for storing data
    data_storage(PageStorage::create(name + ".data",
                                     path_pool.getPSDiskDelegatorSingle("data"),
                                     extractConfig(settings, StorageType::Data),
                                     global_ctx.getFileProvider()))
    ,
    // The iops in meta_storage is relatively high, use multi-disks if possible
    meta_storage(PageStorage::create(name + ".meta",
                                     path_pool.getPSDiskDelegatorMulti("meta"),
                                     extractConfig(settings, StorageType::Meta),
                                     global_ctx.getFileProvider()))
    , log_storage_reader(ns_id, log_storage, nullptr)
    , data_storage_reader(ns_id, data_storage, nullptr)
    , meta_storage_reader(ns_id, meta_storage, nullptr)
    , global_context(global_ctx)
{}

StoragePool::StoragePool(NamespaceId ns_id_, const GlobalStoragePool & global_storage_pool, Context & global_ctx)
    : owned_storage(false)
    , ns_id(ns_id_)
    , log_storage(global_storage_pool.log())
    , data_storage(global_storage_pool.data())
    , meta_storage(global_storage_pool.meta())
    , log_storage_reader(ns_id, log_storage, nullptr)
    , data_storage_reader(ns_id, data_storage, nullptr)
    , meta_storage_reader(ns_id, meta_storage, nullptr)
    , global_context(global_ctx)
{}

void StoragePool::restore()
{
    // If the storage instances is not global, we need to initialize it by ourselves and add a gc task.
    if (owned_storage)
    {
        log_storage->restore();
        data_storage->restore();
        meta_storage->restore();
    }

    max_log_page_id = log_storage->getMaxId(ns_id);
    max_data_page_id = data_storage->getMaxId(ns_id);
    max_meta_page_id = meta_storage->getMaxId(ns_id);
}

StoragePool::~StoragePool()
{
    shutdown();
}

void StoragePool::enableGC()
{
    if (owned_storage)
        gc_handle = global_context.getBackgroundPool().addTask([this] { return this->gc(global_context.getSettingsRef()); });
}

bool StoragePool::gc(const Settings & settings, const Seconds & try_gc_period)
{
    if (!owned_storage)
        return false;

    {
        std::lock_guard lock(mutex);
        // Just do gc for owned storage, otherwise the gc will be handled globally

        Timepoint now = Clock::now();
        if (now < (last_try_gc_time.load() + try_gc_period))
            return false;

        last_try_gc_time = now;
    }

    return doStoragePoolGC(global_context, settings, *this);
}

void StoragePool::shutdown()
{
    if (gc_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        gc_handle = nullptr;
    }
}

void StoragePool::drop()
{
    shutdown();

    if (owned_storage)
    {
        meta_storage->drop();
        data_storage->drop();
        log_storage->drop();
    }
}

PageId StoragePool::newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who)
{
    // In case that there is a DTFile created on disk but TiFlash crashes without persisting the ID.
    // After TiFlash process restored, the ID will be inserted into the stable delegator, but we may
    // get a duplicated ID from the `storage_pool.data`. (tics#2756)
    PageId dtfile_id;
    do
    {
        dtfile_id = ++max_data_page_id;

        auto existed_path = delegator.getDTFilePath(dtfile_id, /*throw_on_not_exist=*/false);
        fiu_do_on(FailPoints::force_set_dtfile_exist_when_acquire_id, {
            static size_t fail_point_called = 0;
            if (existed_path.empty() && fail_point_called % 10 == 0)
            {
                existed_path = "<mock for existed path>";
            }
            fail_point_called++;
        });
        if (likely(existed_path.empty()))
        {
            break;
        }
        // else there is a DTFile with that id, continue to acquire a new ID.
        LOG_FMT_WARNING(&Poco::Logger::get(who),
                        "The DTFile is already exists, continute to acquire another ID. [path={}] [id={}]",
                        existed_path,
                        dtfile_id);
    } while (true);
    return dtfile_id;
}

} // namespace DM
} // namespace DB
