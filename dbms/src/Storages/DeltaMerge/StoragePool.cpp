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
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

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
    config.gc_max_valid_rate = settings.dt_storage_pool_##NAME##_gc_max_valid_rate; \
    config.blob_heavy_gc_valid_rate = settings.dt_storage_blob_heavy_gc_valid_rate; \
    config.blob_block_alignment_bytes = settings.dt_storage_blob_block_alignment_bytes;

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
        throw Exception(fmt::format("Unknown subtype in extractConfig: {} ", static_cast<Int32>(subtype)), ErrorCodes::LOGICAL_ERROR);
    }
#undef SET_CONFIG

    return config;
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
{
}


GlobalStoragePool::~GlobalStoragePool()
{
    if (gc_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        gc_handle = nullptr;
    }
}

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

bool GlobalStoragePool::gc(const Settings & settings, const Seconds & try_gc_period)
{
    // No need lock
    Timepoint now = Clock::now();
    if (now < (last_try_gc_time.load() + try_gc_period))
        return false;

    last_try_gc_time = now;

    bool done_anything = false;
    auto write_limiter = global_context.getWriteLimiter();
    auto read_limiter = global_context.getReadLimiter();
    auto config = extractConfig(settings, StorageType::Meta);
    meta_storage->reloadSettings(config);
    done_anything |= meta_storage->gc(/*not_skip*/ false, write_limiter, read_limiter);

    config = extractConfig(settings, StorageType::Data);
    data_storage->reloadSettings(config);
    done_anything |= data_storage->gc(/*not_skip*/ false, write_limiter, read_limiter);

    config = extractConfig(settings, StorageType::Log);
    log_storage->reloadSettings(config);
    done_anything |= log_storage->gc(/*not_skip*/ false, write_limiter, read_limiter);

    return done_anything;
}

StoragePool::StoragePool(Context & global_ctx, NamespaceId ns_id_, StoragePathPool & storage_path_pool, const String & name)
    : logger(Logger::get("StoragePool", !name.empty() ? name : DB::toString(ns_id_)))
    , run_mode(global_ctx.getPageStorageRunMode())
    , ns_id(ns_id_)
    , global_context(global_ctx)
{
    const auto & global_storage_pool = global_context.getGlobalStoragePool();
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        log_storage_v2 = PageStorage::create(name + ".log",
                                             storage_path_pool.getPSDiskDelegatorMulti("log"),
                                             extractConfig(global_context.getSettingsRef(), StorageType::Log),
                                             global_context.getFileProvider());
        data_storage_v2 = PageStorage::create(name + ".data",
                                              storage_path_pool.getPSDiskDelegatorSingle("data"),
                                              extractConfig(global_context.getSettingsRef(), StorageType::Data),
                                              global_ctx.getFileProvider());
        meta_storage_v2 = PageStorage::create(name + ".meta",
                                              storage_path_pool.getPSDiskDelegatorMulti("meta"),
                                              extractConfig(global_context.getSettingsRef(), StorageType::Meta),
                                              global_ctx.getFileProvider());
        log_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, log_storage_v2, /*storage_v3_*/ nullptr, nullptr);
        data_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, data_storage_v2, /*storage_v3_*/ nullptr, nullptr);
        meta_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, meta_storage_v2, /*storage_v3_*/ nullptr, nullptr);

        log_storage_writer = std::make_shared<PageWriter>(run_mode, log_storage_v2, /*storage_v3_*/ nullptr);
        data_storage_writer = std::make_shared<PageWriter>(run_mode, data_storage_v2, /*storage_v3_*/ nullptr);
        meta_storage_writer = std::make_shared<PageWriter>(run_mode, meta_storage_v2, /*storage_v3_*/ nullptr);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        assert(global_storage_pool != nullptr);
        log_storage_v3 = global_storage_pool->log_storage;
        data_storage_v3 = global_storage_pool->data_storage;
        meta_storage_v3 = global_storage_pool->meta_storage;

        log_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, log_storage_v3, nullptr);
        data_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, data_storage_v3, nullptr);
        meta_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, meta_storage_v3, nullptr);

        log_storage_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, log_storage_v3);
        data_storage_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, data_storage_v3);
        meta_storage_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, meta_storage_v3);

        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        assert(global_storage_pool != nullptr);
        log_storage_v3 = global_storage_pool->log_storage;
        data_storage_v3 = global_storage_pool->data_storage;
        meta_storage_v3 = global_storage_pool->meta_storage;

        log_storage_v2 = PageStorage::create(name + ".log",
                                             storage_path_pool.getPSDiskDelegatorMulti("log"),
                                             extractConfig(global_context.getSettingsRef(), StorageType::Log),
                                             global_context.getFileProvider());
        data_storage_v2 = PageStorage::create(name + ".data",
                                              storage_path_pool.getPSDiskDelegatorMulti("data"),
                                              extractConfig(global_context.getSettingsRef(), StorageType::Data),
                                              global_ctx.getFileProvider());
        meta_storage_v2 = PageStorage::create(name + ".meta",
                                              storage_path_pool.getPSDiskDelegatorMulti("meta"),
                                              extractConfig(global_context.getSettingsRef(), StorageType::Meta),
                                              global_ctx.getFileProvider());

        log_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, log_storage_v2, log_storage_v3, nullptr);
        data_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, data_storage_v2, data_storage_v3, nullptr);
        meta_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, meta_storage_v2, meta_storage_v3, nullptr);

        log_storage_writer = std::make_shared<PageWriter>(run_mode, log_storage_v2, log_storage_v3);
        data_storage_writer = std::make_shared<PageWriter>(run_mode, data_storage_v2, data_storage_v3);
        meta_storage_writer = std::make_shared<PageWriter>(run_mode, meta_storage_v2, meta_storage_v3);
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

PageStorageRunMode StoragePool::restore()
{
    // If the storage instances is not global, we need to initialize it by ourselves and add a gc task.
    if (run_mode == PageStorageRunMode::ONLY_V2 || run_mode == PageStorageRunMode::MIX_MODE)
    {
        log_storage_v2->restore();
        data_storage_v2->restore();
        meta_storage_v2->restore();
    }

    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        max_log_page_id = log_storage_v2->getMaxId(ns_id);
        max_data_page_id = data_storage_v2->getMaxId(ns_id);
        max_meta_page_id = meta_storage_v2->getMaxId(ns_id);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        max_log_page_id = log_storage_v3->getMaxId(ns_id);
        max_data_page_id = data_storage_v3->getMaxId(ns_id);
        max_meta_page_id = meta_storage_v3->getMaxId(ns_id);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        // Check number of valid pages in v2
        if (log_storage_v2->getNumberOfPages() == 0 && data_storage_v2->getNumberOfPages() == 0 && meta_storage_v2->getNumberOfPages() == 0)
        {
            LOG_FMT_INFO(logger, "Current pagestorage change from {} to {}", static_cast<UInt8>(PageStorageRunMode::MIX_MODE), static_cast<UInt8>(PageStorageRunMode::ONLY_V3));
            run_mode = PageStorageRunMode::ONLY_V3;
            log_storage_v2 = nullptr;
            data_storage_v2 = nullptr;
            meta_storage_v2 = nullptr;

            log_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, log_storage_v3, nullptr);
            data_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, data_storage_v3, nullptr);
            meta_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, /*storage_v2_*/ nullptr, meta_storage_v3, nullptr);

            log_storage_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, log_storage_v3);
            data_storage_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, data_storage_v3);
            meta_storage_writer = std::make_shared<PageWriter>(run_mode, /*storage_v2_*/ nullptr, meta_storage_v3);

            max_log_page_id = log_storage_v3->getMaxId(ns_id);
            max_data_page_id = data_storage_v3->getMaxId(ns_id);
            max_meta_page_id = meta_storage_v3->getMaxId(ns_id);
        }
        else
        {
            max_log_page_id = std::max(log_storage_v2->getMaxId(ns_id), log_storage_v3->getMaxId(ns_id));
            max_data_page_id = std::max(data_storage_v2->getMaxId(ns_id), data_storage_v3->getMaxId(ns_id));
            max_meta_page_id = std::max(meta_storage_v2->getMaxId(ns_id), meta_storage_v3->getMaxId(ns_id));
        }
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
    return run_mode;
}

StoragePool::~StoragePool()
{
    shutdown();
}

void StoragePool::enableGC()
{
    if (run_mode == PageStorageRunMode::ONLY_V2 || run_mode == PageStorageRunMode::MIX_MODE)
    {
        gc_handle = global_context.getBackgroundPool().addTask([this] { return this->gc(global_context.getSettingsRef()); });
    }
}

void StoragePool::dataRegisterExternalPagesCallbacks(const ExternalPageCallbacks & callbacks)
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        data_storage_v2->registerExternalPagesCallbacks(callbacks);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        data_storage_v3->registerExternalPagesCallbacks(callbacks);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        // When PageStorage run as Mix Mode.
        // We need both get alive pages from V2 and V3 which will feedback for the DM.
        // But V2 and V3 won't GC in the same time. So V3 need proxy V2 external pages callback.
        // When V3 GC happend, scan the external pages from V3, in remover will scanner all of external pages from V2.
        ExternalPageCallbacks mix_mode_callbacks;

        mix_mode_callbacks.scanner = callbacks.scanner;
        mix_mode_callbacks.remover = [this, callbacks](const ExternalPageCallbacks::PathAndIdsVec & path_and_ids_vec, const std::set<PageId> & valid_ids) {
            // ns_id won't used on V2
            auto v2_valid_page_ids = data_storage_v2->getAliveExternalPageIds(ns_id);
            v2_valid_page_ids.insert(valid_ids.begin(), valid_ids.end());
            callbacks.remover(path_and_ids_vec, v2_valid_page_ids);
        };
        mix_mode_callbacks.ns_id = ns_id;
        data_storage_v3->registerExternalPagesCallbacks(mix_mode_callbacks);
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

void StoragePool::dataUnregisterExternalPagesCallbacks(NamespaceId ns_id)
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        data_storage_v2->unregisterExternalPagesCallbacks(ns_id);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        data_storage_v3->unregisterExternalPagesCallbacks(ns_id);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        // no need unregister callback in V2.
        data_storage_v3->unregisterExternalPagesCallbacks(ns_id);
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}


bool StoragePool::doV2Gc(const Settings & settings)
{
    bool done_anything = false;
    auto write_limiter = global_context.getWriteLimiter();
    auto read_limiter = global_context.getReadLimiter();

    auto config = extractConfig(settings, StorageType::Meta);
    meta_storage_v2->reloadSettings(config);
    done_anything |= meta_storage_v2->gc(/*not_skip*/ false, write_limiter, read_limiter);

    config = extractConfig(settings, StorageType::Data);
    data_storage_v2->reloadSettings(config);
    done_anything |= data_storage_v2->gc(/*not_skip*/ false, write_limiter, read_limiter);

    config = extractConfig(settings, StorageType::Log);
    log_storage_v2->reloadSettings(config);
    done_anything |= log_storage_v2->gc(/*not_skip*/ false, write_limiter, read_limiter);
    return done_anything;
}

bool StoragePool::gc(const Settings & settings, const Seconds & try_gc_period)
{
    if (run_mode == PageStorageRunMode::ONLY_V3)
        return false;

    {
        std::lock_guard lock(mutex);
        // Just do gc for owned storage, otherwise the gc will be handled globally

        Timepoint now = Clock::now();
        if (now < (last_try_gc_time.load() + try_gc_period))
            return false;

        last_try_gc_time = now;
    }

    // Only do the v2 GC
    return doV2Gc(settings);
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

    if (run_mode == PageStorageRunMode::ONLY_V2 || run_mode == PageStorageRunMode::MIX_MODE)
    {
        meta_storage_v2->drop();
        data_storage_v2->drop();
        log_storage_v2->drop();
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
        LOG_FMT_WARNING(logger,
                        "The DTFile is already exists, continute to acquire another ID. [call={}][path={}] [id={}]",
                        who,
                        existed_path,
                        dtfile_id);
    } while (true);
    return dtfile_id;
}

template <typename T>
inline static PageReader newReader(const PageStorageRunMode run_mode, const NamespaceId ns_id, T & storage_v2, T & storage_v3, ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
        return PageReader(run_mode, ns_id, storage_v2, nullptr, snapshot_read ? storage_v2->getSnapshot(tracing_id) : nullptr, read_limiter);
    case PageStorageRunMode::ONLY_V3:
        return PageReader(run_mode, ns_id, nullptr, storage_v3, snapshot_read ? storage_v3->getSnapshot(tracing_id) : nullptr, read_limiter);
    case PageStorageRunMode::MIX_MODE:
        return PageReader(run_mode, ns_id, storage_v2, storage_v3, snapshot_read ? std::make_shared<PageStorageSnapshotMixed>(storage_v2->getSnapshot(fmt::format("{}-v2", tracing_id)), //
                                                                                                                              storage_v3->getSnapshot(fmt::format("{}-v3", tracing_id)))
                                                                                 : nullptr,
                          read_limiter);
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

PageReader StoragePool::newLogReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
{
    return newReader(run_mode, ns_id, log_storage_v2, log_storage_v3, read_limiter, snapshot_read, tracing_id);
}

PageReader StoragePool::newLogReader(ReadLimiterPtr read_limiter, PageStorage::SnapshotPtr & snapshot)
{
    return PageReader(run_mode, ns_id, log_storage_v2, log_storage_v3, snapshot, read_limiter);
}

PageReader StoragePool::newDataReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
{
    return newReader(run_mode, ns_id, data_storage_v2, data_storage_v3, read_limiter, snapshot_read, tracing_id);
}

PageReader StoragePool::newDataReader(ReadLimiterPtr read_limiter, PageStorage::SnapshotPtr & snapshot)
{
    return PageReader(run_mode, ns_id, data_storage_v2, data_storage_v3, snapshot, read_limiter);
}

PageReader StoragePool::newMetaReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
{
    return newReader(run_mode, ns_id, meta_storage_v2, meta_storage_v3, read_limiter, snapshot_read, tracing_id);
}

PageReader StoragePool::newMetaReader(ReadLimiterPtr read_limiter, PageStorage::SnapshotPtr & snapshot)
{
    return PageReader(run_mode, ns_id, meta_storage_v2, meta_storage_v3, snapshot, read_limiter);
}

} // namespace DM
} // namespace DB
