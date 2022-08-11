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

#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V2/PageStorage.h>
#include <fmt/format.h>


namespace CurrentMetrics
{
extern const Metric StoragePoolV2Only;
extern const Metric StoragePoolV3Only;
extern const Metric StoragePoolMixMode;
} // namespace CurrentMetrics

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
    config.blob_heavy_gc_valid_rate = settings.dt_page_gc_threshold;

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
    : log_storage(PageStorage::create("__global__.log",
                                      path_pool.getPSDiskDelegatorGlobalMulti("log"),
                                      extractConfig(settings, StorageType::Log),
                                      global_ctx.getFileProvider(),
                                      true))
    , data_storage(PageStorage::create("__global__.data",
                                       path_pool.getPSDiskDelegatorGlobalMulti("data"),
                                       extractConfig(settings, StorageType::Data),
                                       global_ctx.getFileProvider(),
                                       true))
    , meta_storage(PageStorage::create("__global__.meta",
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

bool GlobalStoragePool::gc()
{
    return gc(global_context.getSettingsRef(), true, DELTA_MERGE_GC_PERIOD);
}

bool GlobalStoragePool::gc(const Settings & settings, bool immediately, const Seconds & try_gc_period)
{
    Timepoint now = Clock::now();
    if (!immediately)
    {
        // No need lock
        if (now < (last_try_gc_time.load() + try_gc_period))
            return false;
    }

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

StoragePool::StoragePool(Context & global_ctx, NamespaceId ns_id_, StoragePathPool & storage_path_pool_, const String & name)
    : logger(Logger::get("StoragePool", !name.empty() ? name : DB::toString(ns_id_)))
    , run_mode(global_ctx.getPageStorageRunMode())
    , ns_id(ns_id_)
    , storage_path_pool(storage_path_pool_)
    , global_context(global_ctx)
    , storage_pool_metrics(CurrentMetrics::StoragePoolV3Only, 0)
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
                                              storage_path_pool.getPSDiskDelegatorSingle("data"), // keep for behavior not changed
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

        if (storage_path_pool.isPSV2Deleted())
        {
            LOG_FMT_INFO(logger, "PageStorage V2 is already mark deleted. Current pagestorage change from {} to {} [ns_id={}]", //
                         static_cast<UInt8>(PageStorageRunMode::MIX_MODE), //
                         static_cast<UInt8>(PageStorageRunMode::ONLY_V3), //
                         ns_id);
            log_storage_v2 = nullptr;
            data_storage_v2 = nullptr;
            meta_storage_v2 = nullptr;
            run_mode = PageStorageRunMode::ONLY_V3;
            storage_path_pool.clearPSV2ObsoleteData();
        }
        else
        {
            // Although there is no more write to ps v2 in mixed mode, the ps instances will keep running if there is some data in log storage when restart,
            // so we keep its original config here.
            // And we rely on the mechanism that writing file will be rotated if no valid pages in non writing files to reduce the disk space usage of these ps instances.
            log_storage_v2 = PageStorage::create(name + ".log",
                                                 storage_path_pool.getPSDiskDelegatorMulti("log"),
                                                 extractConfig(global_context.getSettingsRef(), StorageType::Log),
                                                 global_context.getFileProvider(),
                                                 /* use_v3 */ false,
                                                 /* no_more_write_to_v2 */ true);
            data_storage_v2 = PageStorage::create(name + ".data",
                                                  storage_path_pool.getPSDiskDelegatorMulti("data"),
                                                  extractConfig(global_context.getSettingsRef(), StorageType::Data),
                                                  global_ctx.getFileProvider(),
                                                  /* use_v3 */ false,
                                                  /* no_more_write_to_v2 */ true);
            meta_storage_v2 = PageStorage::create(name + ".meta",
                                                  storage_path_pool.getPSDiskDelegatorMulti("meta"),
                                                  extractConfig(global_context.getSettingsRef(), StorageType::Meta),
                                                  global_ctx.getFileProvider(),
                                                  /* use_v3 */ false,
                                                  /* no_more_write_to_v2 */ true);
        }

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

void StoragePool::forceTransformMetaV2toV3()
{
    if (unlikely(run_mode != PageStorageRunMode::MIX_MODE))
        throw Exception(fmt::format("Transform meta must run under mix mode [run_mode={}]", static_cast<Int32>(run_mode)));
    assert(meta_storage_v2 != nullptr);
    assert(meta_storage_v3 != nullptr);
    auto meta_transform_storage_writer = std::make_shared<PageWriter>(run_mode, meta_storage_v2, meta_storage_v3);
    auto meta_transform_storage_reader = std::make_shared<PageReader>(run_mode, ns_id, meta_storage_v2, meta_storage_v3, nullptr);

    Pages pages_transform = {};
    auto meta_transform_acceptor = [&](const DB::Page & page) {
        pages_transform.emplace_back(page);
    };

    meta_transform_storage_reader->traverse(meta_transform_acceptor, /*only_v2*/ true, /*only_v3*/ false);

    WriteBatch write_batch_transform{ns_id};
    WriteBatch write_batch_del_v2{ns_id};

    for (const auto & page_transform : pages_transform)
    {
        // Check pages have not contain field offset
        // Also get the tag of page_id
        const auto & page_transform_entry = meta_transform_storage_reader->getPageEntry(page_transform.page_id);
        if (!page_transform_entry.field_offsets.empty())
        {
            throw Exception(fmt::format("Can't transform meta from V2 to V3, [page_id={}] {}", //
                                        page_transform.page_id,
                                        page_transform_entry.toDebugString()),
                            ErrorCodes::LOGICAL_ERROR);
        }

        write_batch_transform.putPage(page_transform.page_id, //
                                      page_transform_entry.tag,
                                      std::make_shared<ReadBufferFromMemory>(page_transform.data.begin(),
                                                                             page_transform.data.size()),
                                      page_transform.data.size());
        // Record del for V2
        write_batch_del_v2.delPage(page_transform.page_id);
    }

    // Will rewrite into V3.
    meta_transform_storage_writer->write(std::move(write_batch_transform), nullptr);

    // DEL must call after rewrite.
    meta_transform_storage_writer->writeIntoV2(std::move(write_batch_del_v2), nullptr);
}

static inline DB::PS::V2::PageEntriesVersionSetWithDelta::Snapshot *
toV2ConcreteSnapshot(const DB::PageStorage::SnapshotPtr & ptr)
{
    return dynamic_cast<DB::PS::V2::PageEntriesVersionSetWithDelta::Snapshot *>(ptr.get());
}

void StoragePool::forceTransformDataV2toV3()
{
    if (unlikely(run_mode != PageStorageRunMode::MIX_MODE))
        throw Exception(fmt::format("Transform meta must run under mix mode [run_mode={}]", static_cast<Int32>(run_mode)));
    assert(data_storage_v2 != nullptr);
    assert(data_storage_v3 != nullptr);
    auto data_transform_storage_writer = std::make_shared<PageWriter>(run_mode, data_storage_v2, data_storage_v3);

    auto snapshot = data_storage_v2->getSnapshot("transformDataV2toV3");
    auto * v2_snap = toV2ConcreteSnapshot(snapshot);
    if (!snapshot || !v2_snap)
    {
        throw Exception("Can not allocate snapshot from pool.data v2", ErrorCodes::LOGICAL_ERROR);
    }

    // Example
    // 100 -> 100
    // 102 -> 100
    // 105 -> 100
    // 200 -> 200
    // 305 -> 300
    // Migration steps:
    // collect v2 valid page id: 100, 102, 105, 200, 305
    // v3 put external 100, 200, 300; put ref 102, 105, 305
    // mark some id as deleted: v3 del 300
    // v2 delete 100, 102, 105, 200, 305

    // The page ids that can be accessed by DeltaTree
    const auto all_page_ids = v2_snap->view.validPageIds();

    WriteBatch write_batch_transform{ns_id};
    WriteBatch write_batch_del_v2{ns_id};

    std::set<PageId> created_dt_file_id;
    for (const auto page_id : all_page_ids)
    {
        // resolve the page_id into dtfile id
        const auto resolved_file_id = v2_snap->view.resolveRefId(page_id);
        if (auto ins_result = created_dt_file_id.insert(resolved_file_id); /*created=*/ins_result.second)
        {
            // first see this file id, migrate to v3
            write_batch_transform.putExternal(resolved_file_id, 0);
        }
        // migrate the reference for v3
        if (page_id != resolved_file_id)
        {
            write_batch_transform.putRefPage(page_id, resolved_file_id);
        }
        // record del for V2
        write_batch_del_v2.delPage(page_id);
    }
    // If the file id is not existed in `all_page_ids`, it means the file id
    // itself has been deleted.
    for (const auto dt_file_id : created_dt_file_id)
    {
        if (all_page_ids.count(dt_file_id) == 0)
        {
            write_batch_transform.delPage(dt_file_id);
        }
    }

    // Will rewrite into V3.
    data_transform_storage_writer->writeIntoV3(std::move(write_batch_transform), nullptr);

    // DEL must call after rewrite.
    data_transform_storage_writer->writeIntoV2(std::move(write_batch_del_v2), nullptr);
}

PageStorageRunMode StoragePool::restore()
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        log_storage_v2->restore();
        data_storage_v2->restore();
        meta_storage_v2->restore();

        max_log_page_id = log_storage_v2->getMaxId();
        max_data_page_id = data_storage_v2->getMaxId();
        max_meta_page_id = meta_storage_v2->getMaxId();

        storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolV2Only};
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        max_log_page_id = log_storage_v3->getMaxId();
        max_data_page_id = data_storage_v3->getMaxId();
        max_meta_page_id = meta_storage_v3->getMaxId();

        storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolV3Only};
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        log_storage_v2->restore();
        data_storage_v2->restore();
        meta_storage_v2->restore();

        // The pages on data and log can be rewritten to V3 and the old pages on V2 are deleted by `delta merge`.
        // However, the pages on meta V2 can not be deleted. As the pages in meta are small, we perform a forceTransformMetaV2toV3 to convert pages before all.
        if (const auto & meta_remain_pages = meta_storage_v2->getNumberOfPages(); meta_remain_pages != 0)
        {
            LOG_FMT_INFO(logger, "Current pool.meta transform to V3 begin [ns_id={}] [pages_before_transform={}]", ns_id, meta_remain_pages);
            forceTransformMetaV2toV3();
            const auto & meta_remain_pages_after_transform = meta_storage_v2->getNumberOfPages();
            LOG_FMT_INFO(logger, "Current pool.meta transform to V3 finished [ns_id={}] [done={}] [pages_before_transform={}], [pages_after_transform={}]", //
                         ns_id,
                         meta_remain_pages_after_transform == 0,
                         meta_remain_pages,
                         meta_remain_pages_after_transform);
        }
        else
        {
            LOG_FMT_INFO(logger, "Current pool.meta translate already done before restored [ns_id={}] ", ns_id);
        }

        if (const auto & data_remain_pages = data_storage_v2->getNumberOfPages(); data_remain_pages != 0)
        {
            LOG_FMT_INFO(logger, "Current pool.data transform to V3 begin [ns_id={}] [pages_before_transform={}]", ns_id, data_remain_pages);
            forceTransformDataV2toV3();
            const auto & data_remain_pages_after_transform = data_storage_v2->getNumberOfPages();
            LOG_FMT_INFO(logger, "Current pool.data transform to V3 finished [ns_id={}] [done={}] [pages_before_transform={}], [pages_after_transform={}]", //
                         ns_id,
                         data_remain_pages_after_transform == 0,
                         data_remain_pages,
                         data_remain_pages_after_transform);
        }
        else
        {
            LOG_FMT_INFO(logger, "Current pool.data translate already done before restored [ns_id={}]", ns_id);
        }

        // Check number of valid pages in v2
        // If V2 already have no any data in disk, Then change run_mode to ONLY_V3
        if (log_storage_v2->getNumberOfPages() == 0 && data_storage_v2->getNumberOfPages() == 0 && meta_storage_v2->getNumberOfPages() == 0)
        {
            LOG_FMT_INFO(logger, "Current pagestorage change from {} to {} [ns_id={}]", //
                         static_cast<UInt8>(PageStorageRunMode::MIX_MODE),
                         static_cast<UInt8>(PageStorageRunMode::ONLY_V3),
                         ns_id);
            if (storage_path_pool.createPSV2DeleteMarkFile())
            {
                log_storage_v2->drop();
                data_storage_v2->drop();
                meta_storage_v2->drop();
            }
            log_storage_v2 = nullptr;
            data_storage_v2 = nullptr;
            meta_storage_v2 = nullptr;

            // Must init by PageStorageRunMode::ONLY_V3
            log_storage_reader = std::make_shared<PageReader>(PageStorageRunMode::ONLY_V3, ns_id, /*storage_v2_*/ nullptr, log_storage_v3, nullptr);
            data_storage_reader = std::make_shared<PageReader>(PageStorageRunMode::ONLY_V3, ns_id, /*storage_v2_*/ nullptr, data_storage_v3, nullptr);
            meta_storage_reader = std::make_shared<PageReader>(PageStorageRunMode::ONLY_V3, ns_id, /*storage_v2_*/ nullptr, meta_storage_v3, nullptr);

            log_storage_writer = std::make_shared<PageWriter>(PageStorageRunMode::ONLY_V3, /*storage_v2_*/ nullptr, log_storage_v3);
            data_storage_writer = std::make_shared<PageWriter>(PageStorageRunMode::ONLY_V3, /*storage_v2_*/ nullptr, data_storage_v3);
            meta_storage_writer = std::make_shared<PageWriter>(PageStorageRunMode::ONLY_V3, /*storage_v2_*/ nullptr, meta_storage_v3);

            max_log_page_id = log_storage_v3->getMaxId();
            max_data_page_id = data_storage_v3->getMaxId();
            max_meta_page_id = meta_storage_v3->getMaxId();

            run_mode = PageStorageRunMode::ONLY_V3;
            storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolV3Only};
        }
        else // Still running Mix Mode
        {
            max_log_page_id = std::max(log_storage_v2->getMaxId(), log_storage_v3->getMaxId());
            max_data_page_id = std::max(data_storage_v2->getMaxId(), data_storage_v3->getMaxId());
            max_meta_page_id = std::max(meta_storage_v2->getMaxId(), meta_storage_v3->getMaxId());
            storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolMixMode};
        }
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
    LOG_FMT_TRACE(logger, "Finished StoragePool restore. [current_run_mode={}] [ns_id={}]"
                          " [max_log_page_id={}] [max_data_page_id={}] [max_meta_page_id={}]",
                  static_cast<UInt8>(run_mode),
                  ns_id,
                  max_log_page_id,
                  max_data_page_id,
                  max_meta_page_id);
    return run_mode;
}

StoragePool::~StoragePool()
{
    shutdown();
}

void StoragePool::enableGC()
{
    // The data in V3 will be GCed by `GlobalStoragePool::gc`, only register gc task under only v2/mix mode
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
        // We have transformed all pages from V2 to V3 in `restore`, so
        // only need to register callbacks for V3.
        data_storage_v3->registerExternalPagesCallbacks(callbacks);
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
            static std::atomic<UInt64> fail_point_called(0);
            if (existed_path.empty() && fail_point_called.load() % 10 == 0)
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
