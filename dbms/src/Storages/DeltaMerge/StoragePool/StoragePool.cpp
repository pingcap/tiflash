// Copyright 2024 PingCAP, Inc.
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
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/StoragePool/GlobalStoragePool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePoolConfig.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>
#include <fmt/format.h>

#include <magic_enum.hpp>

namespace CurrentMetrics
{
extern const Metric StoragePoolV2Only;
extern const Metric StoragePoolV3Only;
extern const Metric StoragePoolMixMode;
extern const Metric StoragePoolUniPS;
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
} // namespace DB

namespace DB::DM
{

StoragePool::StoragePool(
    Context & global_ctx,
    KeyspaceID keyspace_id_,
    NamespaceID table_id_,
    StoragePathPool & storage_path_pool_,
    const String & name)
    : StoragePool(global_ctx, keyspace_id_, table_id_, storage_path_pool_, global_ctx.getGlobalPageIdAllocator(), name)
{
    //
}

StoragePool::StoragePool(
    Context & global_ctx,
    KeyspaceID keyspace_id_,
    NamespaceID table_id_,
    StoragePathPool & storage_path_pool_,
    GlobalPageIdAllocatorPtr page_id_allocator_,
    const String & name)
    : logger(Logger::get(!name.empty() ? name : DB::toString(table_id_)))
    , run_mode(global_ctx.getPageStorageRunMode())
    , keyspace_id(keyspace_id_)
    , table_id(table_id_)
    , storage_path_pool(storage_path_pool_)
    , uni_ps(run_mode == PageStorageRunMode::UNI_PS ? global_ctx.getWriteNodePageStorage() : nullptr)
    , global_context(global_ctx)
    , global_id_allocator(std::move(page_id_allocator_))
    , storage_pool_metrics(CurrentMetrics::StoragePoolV3Only, 0)
{
    assert(global_id_allocator != nullptr);
    const auto & global_storage_pool = global_context.getGlobalStoragePool();
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        log_storage_v2 = PageStorage::create(
            name + ".log",
            storage_path_pool.getPSDiskDelegatorMulti("log"),
            extractConfig(global_context.getSettingsRef(), StorageType::Log),
            global_context.getFileProvider(),
            global_context);
        data_storage_v2 = PageStorage::create(
            name + ".data",
            storage_path_pool.getPSDiskDelegatorSingle("data"), // keep for behavior not changed
            extractConfig(global_context.getSettingsRef(), StorageType::Data),
            global_context.getFileProvider(),
            global_context);
        meta_storage_v2 = PageStorage::create(
            name + ".meta",
            storage_path_pool.getPSDiskDelegatorMulti("meta"),
            extractConfig(global_context.getSettingsRef(), StorageType::Meta),
            global_context.getFileProvider(),
            global_context);
        log_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Log,
            table_id,
            log_storage_v2,
            /*storage_v3_*/ nullptr,
            /*uni_ps_*/ nullptr,
            nullptr);
        data_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Data,
            table_id,
            data_storage_v2,
            /*storage_v3_*/ nullptr,
            /*uni_ps_*/ nullptr,
            nullptr);
        meta_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Meta,
            table_id,
            meta_storage_v2,
            /*storage_v3_*/ nullptr,
            /*uni_ps_*/ nullptr,
            nullptr);

        log_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Log,
            log_storage_v2,
            /*storage_v3_*/ nullptr,
            /*uni_ps_*/ nullptr);
        data_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Data,
            data_storage_v2,
            /*storage_v3_*/ nullptr,
            /*uni_ps_*/ nullptr);
        meta_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Meta,
            meta_storage_v2,
            /*storage_v3_*/ nullptr,
            /*uni_ps_*/ nullptr);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        assert(global_storage_pool != nullptr);
        log_storage_v3 = global_storage_pool->log_storage;
        data_storage_v3 = global_storage_pool->data_storage;
        meta_storage_v3 = global_storage_pool->meta_storage;

        log_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Log,
            table_id,
            /*storage_v2_*/ nullptr,
            log_storage_v3,
            /*uni_ps_*/ nullptr,
            nullptr);
        data_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Data,
            table_id,
            /*storage_v2_*/ nullptr,
            data_storage_v3,
            /*uni_ps_*/ nullptr,
            nullptr);
        meta_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Meta,
            table_id,
            /*storage_v2_*/ nullptr,
            meta_storage_v3,
            /*uni_ps_*/ nullptr,
            nullptr);

        log_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Log,
            /*storage_v2_*/ nullptr,
            log_storage_v3,
            /*uni_ps_*/ nullptr);
        data_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Data,
            /*storage_v2_*/ nullptr,
            data_storage_v3,
            /*uni_ps_*/ nullptr);
        meta_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Meta,
            /*storage_v2_*/ nullptr,
            meta_storage_v3,
            /*uni_ps_*/ nullptr);
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
            LOG_INFO(
                logger,
                "PageStorage V2 is already mark deleted. Current pagestorage change from {} to {}, table_id={}",
                magic_enum::enum_name(PageStorageRunMode::MIX_MODE),
                magic_enum::enum_name(PageStorageRunMode::ONLY_V3),
                table_id);
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
            log_storage_v2 = PageStorage::create(
                name + ".log",
                storage_path_pool.getPSDiskDelegatorMulti("log"),
                extractConfig(global_context.getSettingsRef(), StorageType::Log),
                global_context.getFileProvider(),
                global_context,
                /* use_v3 */ false,
                /* no_more_write_to_v2 */ true);
            data_storage_v2 = PageStorage::create(
                name + ".data",
                storage_path_pool.getPSDiskDelegatorMulti("data"),
                extractConfig(global_context.getSettingsRef(), StorageType::Data),
                global_context.getFileProvider(),
                global_context,
                /* use_v3 */ false,
                /* no_more_write_to_v2 */ true);
            meta_storage_v2 = PageStorage::create(
                name + ".meta",
                storage_path_pool.getPSDiskDelegatorMulti("meta"),
                extractConfig(global_context.getSettingsRef(), StorageType::Meta),
                global_context.getFileProvider(),
                global_context,
                /* use_v3 */ false,
                /* no_more_write_to_v2 */ true);
        }

        log_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Log,
            table_id,
            log_storage_v2,
            log_storage_v3,
            /*uni_ps_*/ nullptr,
            nullptr);
        data_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Data,
            table_id,
            data_storage_v2,
            data_storage_v3,
            /*uni_ps_*/ nullptr,
            nullptr);
        meta_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Meta,
            table_id,
            meta_storage_v2,
            meta_storage_v3,
            /*uni_ps_*/ nullptr,
            nullptr);

        log_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Log,
            log_storage_v2,
            log_storage_v3,
            /*uni_ps_*/ nullptr);
        data_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Data,
            data_storage_v2,
            data_storage_v3,
            /*uni_ps_*/ nullptr);
        meta_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Meta,
            meta_storage_v2,
            meta_storage_v3,
            /*uni_ps_*/ nullptr);
        break;
    }
    case PageStorageRunMode::UNI_PS:
    {
        log_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Log,
            table_id,
            /*storage_v2_*/ nullptr,
            /*storage_v3_*/ nullptr,
            uni_ps,
            nullptr);
        data_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Data,
            table_id,
            /*storage_v2_*/ nullptr,
            /*storage_v3_*/ nullptr,
            uni_ps,
            nullptr);
        meta_storage_reader = std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            StorageType::Meta,
            table_id,
            /*storage_v2_*/ nullptr,
            /*storage_v3_*/ nullptr,
            uni_ps,
            nullptr);

        log_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Log,
            /*storage_v2_*/ nullptr,
            /*storage_v3_*/ nullptr,
            uni_ps);
        data_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Data,
            /*storage_v2_*/ nullptr,
            /*storage_v3_*/ nullptr,
            uni_ps);
        meta_storage_writer = std::make_shared<PageWriter>(
            run_mode,
            StorageType::Meta,
            /*storage_v2_*/ nullptr,
            /*storage_v3_*/ nullptr,
            uni_ps);
        break;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode));
    }
}

void StoragePool::forceTransformMetaV2toV3()
{
    RUNTIME_CHECK_MSG(
        run_mode == PageStorageRunMode::MIX_MODE,
        "Transform meta must run under mix mode, run_mode={}",
        static_cast<Int32>(run_mode));

    assert(meta_storage_v2 != nullptr);
    assert(meta_storage_v3 != nullptr);
    auto meta_transform_storage_writer = std::make_shared<PageWriter>(
        run_mode,
        StorageType::Meta,
        meta_storage_v2,
        meta_storage_v3,
        /*uni_ps_*/ nullptr);
    auto meta_transform_storage_reader = std::make_shared<PageReader>(
        run_mode,
        keyspace_id,
        StorageType::Meta,
        table_id,
        meta_storage_v2,
        meta_storage_v3,
        /*uni_ps_*/ nullptr,
        nullptr);

    Pages pages_transform = {};
    auto meta_transform_acceptor = [&](const DB::Page & page) {
        pages_transform.emplace_back(page);
    };

    meta_transform_storage_reader->traverse(meta_transform_acceptor, /*only_v2*/ true, /*only_v3*/ false);

    WriteBatch write_batch_transform{table_id};
    WriteBatch write_batch_del_v2{table_id};

    for (const auto & page_transform : pages_transform)
    {
        // Check pages have not contain field offset
        // Also get the tag of page_id
        const auto & page_transform_entry = meta_transform_storage_reader->getPageEntry(page_transform.page_id);
        if (!page_transform_entry.field_offsets.empty())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can't transform meta from V2 to V3, [page_id={}] {}", //
                page_transform.page_id,
                page_transform_entry.toDebugString());
        }

        write_batch_transform.putPage(
            page_transform.page_id, //
            page_transform_entry.tag,
            std::make_shared<ReadBufferFromMemory>(page_transform.data.begin(), page_transform.data.size()),
            page_transform.data.size());
        // Record del for V2
        write_batch_del_v2.delPage(page_transform.page_id);
    }

    // Will rewrite into V3.
    meta_transform_storage_writer->writeIntoV3(std::move(write_batch_transform), nullptr);

    // DEL must call after rewrite.
    meta_transform_storage_writer->writeIntoV2(std::move(write_batch_del_v2), nullptr);
}

static inline DB::PS::V2::PageEntriesVersionSetWithDelta::Snapshot * toV2ConcreteSnapshot(
    const DB::PageStorage::SnapshotPtr & ptr)
{
    return dynamic_cast<DB::PS::V2::PageEntriesVersionSetWithDelta::Snapshot *>(ptr.get());
}

void StoragePool::forceTransformDataV2toV3()
{
    RUNTIME_CHECK_MSG(
        run_mode == PageStorageRunMode::MIX_MODE,
        "Transform data must run under mix mode, run_mode={}",
        static_cast<Int32>(run_mode));

    assert(data_storage_v2 != nullptr);
    assert(data_storage_v3 != nullptr);
    auto data_transform_storage_writer = std::make_shared<PageWriter>(
        run_mode,
        StorageType::Data,
        data_storage_v2,
        data_storage_v3,
        /*uni_ps_*/ nullptr);

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

    WriteBatch write_batch_transform{table_id};
    WriteBatch write_batch_del_v2{table_id};

    PageIdU64Set created_dt_file_id;
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
        // Restore the PSV2 instances from disk
        log_storage_v2->restore();
        data_storage_v2->restore();
        meta_storage_v2->restore();

        // ONLY_V2, make sure the page_ids is larger than that in PSV2
        global_id_allocator->raiseLogPageIdLowerBound(log_storage_v2->getMaxId());
        global_id_allocator->raiseDataPageIdLowerBound(data_storage_v2->getMaxId());
        global_id_allocator->raiseMetaPageIdLowerBound(meta_storage_v2->getMaxId());

        storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolV2Only};
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        // ONLY_V3
        // - StoragePool is simply a wrapper for the PS instances in GlobalStoragePool
        // - Make sure the page_ids is larger than that in PSV2
        global_id_allocator->raiseLogPageIdLowerBound(log_storage_v3->getMaxId());
        global_id_allocator->raiseDataPageIdLowerBound(data_storage_v3->getMaxId());
        global_id_allocator->raiseMetaPageIdLowerBound(meta_storage_v3->getMaxId());

        storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolV3Only};
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        // Restore the PSV2 instances from disk
        log_storage_v2->restore();
        data_storage_v2->restore();
        meta_storage_v2->restore();

        // The pages on data and log can be rewritten to V3 and the old pages on V2 are deleted by `delta merge`.
        // However, the pages on meta V2 can not be deleted. As the pages in meta are small, we perform a forceTransformMetaV2toV3 to convert pages before all.
        if (const auto & meta_remain_pages = meta_storage_v2->getNumberOfPages(); meta_remain_pages != 0)
        {
            LOG_INFO(
                logger,
                "Current pool.meta transform to V3 begin [table_id={}] [pages_before_transform={}]",
                table_id,
                meta_remain_pages);
            forceTransformMetaV2toV3();
            const auto & meta_remain_pages_after_transform = meta_storage_v2->getNumberOfPages();
            LOG_INFO(
                logger,
                "Current pool.meta transform to V3 finished [table_id={}] [done={}] [pages_before_transform={}], "
                "[pages_after_transform={}]", //
                table_id,
                meta_remain_pages_after_transform == 0,
                meta_remain_pages,
                meta_remain_pages_after_transform);
        }
        else
        {
            LOG_INFO(logger, "Current pool.meta transform already done before restored [table_id={}] ", table_id);
        }

        if (const auto & data_remain_pages = data_storage_v2->getNumberOfPages(); data_remain_pages != 0)
        {
            LOG_INFO(
                logger,
                "Current pool.data transform to V3 begin [table_id={}] [pages_before_transform={}]",
                table_id,
                data_remain_pages);
            forceTransformDataV2toV3();
            const auto & data_remain_pages_after_transform = data_storage_v2->getNumberOfPages();
            LOG_INFO(
                logger,
                "Current pool.data transform to V3 finished [table_id={}] [done={}] [pages_before_transform={}], "
                "[pages_after_transform={}]", //
                table_id,
                data_remain_pages_after_transform == 0,
                data_remain_pages,
                data_remain_pages_after_transform);
        }
        else
        {
            LOG_INFO(logger, "Current pool.data transform already done before restored [table_id={}]", table_id);
        }

        // Though all the pages may have been transformed into PageStoage V3 format, we still need
        // to ensure the following allocated page_ids is larger than that in both v2 and v3.
        // Because `PageStorageV3->getMaxId` is not accurate after the previous "meta" and "data"
        // transformed from v2 to v3.
        auto max_log_page_id = std::max(log_storage_v2->getMaxId(), log_storage_v3->getMaxId());
        auto max_data_page_id = std::max(data_storage_v2->getMaxId(), data_storage_v3->getMaxId());
        auto max_meta_page_id = std::max(meta_storage_v2->getMaxId(), meta_storage_v3->getMaxId());
        global_id_allocator->raiseLogPageIdLowerBound(max_log_page_id);
        global_id_allocator->raiseDataPageIdLowerBound(max_data_page_id);
        global_id_allocator->raiseMetaPageIdLowerBound(max_meta_page_id);

        // Check number of valid pages in v2
        // If V2 already have no any data in disk, Then change run_mode to ONLY_V3
        if (log_storage_v2->getNumberOfPages() == 0 && data_storage_v2->getNumberOfPages() == 0
            && meta_storage_v2->getNumberOfPages() == 0)
        {
            LOG_INFO(
                logger,
                "Current pagestorage change from {} to {} [table_id={}]",
                magic_enum::enum_name(PageStorageRunMode::MIX_MODE),
                magic_enum::enum_name(PageStorageRunMode::ONLY_V3),
                table_id);

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
            log_storage_reader = std::make_shared<PageReader>(
                PageStorageRunMode::ONLY_V3,
                keyspace_id,
                StorageType::Log,
                table_id,
                /*storage_v2_*/ nullptr,
                log_storage_v3,
                /*uni_ps_*/ nullptr,
                nullptr);
            data_storage_reader = std::make_shared<PageReader>(
                PageStorageRunMode::ONLY_V3,
                keyspace_id,
                StorageType::Data,
                table_id,
                /*storage_v2_*/ nullptr,
                data_storage_v3,
                /*uni_ps_*/ nullptr,
                nullptr);
            meta_storage_reader = std::make_shared<PageReader>(
                PageStorageRunMode::ONLY_V3,
                keyspace_id,
                StorageType::Meta,
                table_id,
                /*storage_v2_*/ nullptr,
                meta_storage_v3,
                /*uni_ps_*/ nullptr,
                nullptr);

            log_storage_writer = std::make_shared<PageWriter>(
                PageStorageRunMode::ONLY_V3,
                StorageType::Log,
                /*storage_v2_*/ nullptr,
                log_storage_v3,
                /*uni_ps_*/ nullptr);
            data_storage_writer = std::make_shared<PageWriter>(
                PageStorageRunMode::ONLY_V3,
                StorageType::Data,
                /*storage_v2_*/ nullptr,
                data_storage_v3,
                /*uni_ps_*/ nullptr);
            meta_storage_writer = std::make_shared<PageWriter>(
                PageStorageRunMode::ONLY_V3,
                StorageType::Meta,
                /*storage_v2_*/ nullptr,
                meta_storage_v3,
                /*uni_ps_*/ nullptr);

            run_mode = PageStorageRunMode::ONLY_V3;
            storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolV3Only};
        }
        else // Still running Mix Mode
        {
            storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolMixMode};
        }
        break;
    }
    case PageStorageRunMode::UNI_PS:
    {
        // UNI_PS
        // - StoragePool is simply a wrapper for the uni_ps
        auto max_page_id = uni_ps->getMaxIdAfterRestart();
        global_id_allocator->raiseLogPageIdLowerBound(max_page_id);
        global_id_allocator->raiseDataPageIdLowerBound(max_page_id);
        global_id_allocator->raiseMetaPageIdLowerBound(max_page_id);

        storage_pool_metrics = CurrentMetrics::Increment{CurrentMetrics::StoragePoolUniPS};
        break;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode));
    }

    const auto [max_log_page_id, max_data_page_id, max_meta_page_id] = global_id_allocator->getCurrentIds();
    LOG_INFO(
        logger,
        "Finished StoragePool restore. run_mode={} keyspace_id={} table_id={}"
        " max_log_page_id={} max_data_page_id={} max_meta_page_id={}",
        magic_enum::enum_name(run_mode),
        keyspace_id,
        table_id,
        max_log_page_id,
        max_data_page_id,
        max_meta_page_id);
    return run_mode;
}

StoragePool::~StoragePool()
{
    shutdown();
}

void StoragePool::startup(ExternalPageCallbacks && callbacks)
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        // For V2, we need a per physical table gc handle to perform the gc of its PageStorage instances.
        data_storage_v2->registerExternalPagesCallbacks(callbacks);
        gc_handle
            = global_context.getBackgroundPool().addTask([this] { return this->gc(global_context.getSettingsRef()); });
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        // For V3, the GC is handled by `GlobalStoragePool::gc`, just register callbacks is OK.
        data_storage_v3->registerExternalPagesCallbacks(callbacks);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        // For V3, the GC is handled by `GlobalStoragePool::gc`.
        // Since we have transformed all external pages from V2 to V3 in `StoragePool::restore`,
        // just register callbacks to V3 is OK
        data_storage_v3->registerExternalPagesCallbacks(callbacks);
        // we still need a gc_handle to reclaim the V2 disk space.
        gc_handle
            = global_context.getBackgroundPool().addTask([this] { return this->gc(global_context.getSettingsRef()); });
        break;
    }
    case PageStorageRunMode::UNI_PS:
    {
        // For uni ps, the GC is handled by `UniversalPageStorageService`, register callbacks with prefix for this table
        UniversalExternalPageCallbacks us_callbacks;
        us_callbacks.remover = std::move(callbacks.remover);
        us_callbacks.scanner = std::move(callbacks.scanner);
        us_callbacks.prefix = UniversalPageIdFormat::toFullPrefix(keyspace_id, StorageType::Data, table_id);
        uni_ps->registerUniversalExternalPagesCallbacks(us_callbacks);
        break;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode));
    }
}

void StoragePool::shutdown()
{
    // Note: Should reset the gc_handle before unregistering the pages callbacks
    if (gc_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        gc_handle = nullptr;
    }

    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        meta_storage_v2->shutdown();
        log_storage_v2->shutdown();
        data_storage_v2->shutdown();
        data_storage_v2->unregisterExternalPagesCallbacks(table_id);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        data_storage_v3->unregisterExternalPagesCallbacks(table_id);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        meta_storage_v2->shutdown();
        log_storage_v2->shutdown();
        data_storage_v2->shutdown();
        // We have transformed all external pages from V2 to V3 in `restore`, so
        // only need to unregister callbacks for V3.
        data_storage_v3->unregisterExternalPagesCallbacks(table_id);
        break;
    }
    case PageStorageRunMode::UNI_PS:
    {
        uni_ps->unregisterUniversalExternalPagesCallbacks(
            UniversalPageIdFormat::toFullPrefix(keyspace_id, StorageType::Data, table_id));
        break;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode));
    }
}

bool StoragePool::doV2Gc(const Settings & settings) const
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
    if (run_mode == PageStorageRunMode::ONLY_V3 || run_mode == PageStorageRunMode::UNI_PS)
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

PageIdU64 StoragePool::newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who) const
{
    // In case that there is a DTFile created on disk but TiFlash crashes without persisting the ID.
    // After TiFlash process restored, the ID will be inserted into the stable delegator, but we may
    // get a duplicated ID from the `storage_pool.data`. (tics#2756)
    PageIdU64 dtfile_id;
    do
    {
        dtfile_id = global_id_allocator->newDataPageIdForDTFile();

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
        LOG_WARNING(
            logger,
            "The DTFile is already exists, continute to acquire another ID. call={} path={} file_id={}",
            who,
            existed_path,
            dtfile_id);
    } while (true);
    return dtfile_id;
}

PageIdU64 StoragePool::newLogPageId() const
{
    return global_id_allocator->newLogPageId();
}

PageIdU64 StoragePool::newMetaPageId() const
{
    return global_id_allocator->newMetaPageId();
}

template <typename T>
inline static PageReaderPtr newReader(
    const PageStorageRunMode run_mode,
    KeyspaceID keyspace_id,
    StorageType tag,
    const NamespaceID table_id,
    T & storage_v2,
    T & storage_v3,
    UniversalPageStoragePtr uni_ps,
    ReadLimiterPtr read_limiter,
    bool snapshot_read,
    const String & tracing_id)
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
        return std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            tag,
            table_id,
            storage_v2,
            nullptr,
            /*uni_ps*/ nullptr,
            snapshot_read ? storage_v2->getSnapshot(tracing_id) : nullptr,
            read_limiter);
    case PageStorageRunMode::ONLY_V3:
        return std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            tag,
            table_id,
            nullptr,
            storage_v3,
            /*uni_ps*/ nullptr,
            snapshot_read ? storage_v3->getSnapshot(tracing_id) : nullptr,
            read_limiter);
    case PageStorageRunMode::MIX_MODE:
        return std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            tag,
            table_id,
            storage_v2,
            storage_v3,
            /*uni_ps*/ nullptr,
            snapshot_read ? std::make_shared<PageStorageSnapshotMixed>(
                storage_v2->getSnapshot(fmt::format("{}-v2", tracing_id)),
                storage_v3->getSnapshot(fmt::format("{}-v3", tracing_id)))
                          : nullptr,
            read_limiter);
    case PageStorageRunMode::UNI_PS:
        return std::make_shared<PageReader>(
            run_mode,
            keyspace_id,
            tag,
            table_id,
            nullptr,
            nullptr,
            uni_ps,
            snapshot_read ? uni_ps->getSnapshot(tracing_id) : nullptr,
            read_limiter);
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode));
    }
}

PageReaderPtr StoragePool::newLogReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
{
    return newReader(
        run_mode,
        keyspace_id,
        StorageType::Log,
        table_id,
        log_storage_v2,
        log_storage_v3,
        uni_ps,
        read_limiter,
        snapshot_read,
        tracing_id);
}

PageReaderPtr StoragePool::newLogReader(ReadLimiterPtr read_limiter, PageStorage::SnapshotPtr & snapshot)
{
    return std::make_shared<PageReader>(
        run_mode,
        keyspace_id,
        StorageType::Log,
        table_id,
        log_storage_v2,
        log_storage_v3,
        uni_ps,
        snapshot,
        read_limiter);
}

PageReaderPtr StoragePool::newDataReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
{
    return newReader(
        run_mode,
        keyspace_id,
        StorageType::Data,
        table_id,
        data_storage_v2,
        data_storage_v3,
        uni_ps,
        read_limiter,
        snapshot_read,
        tracing_id);
}

PageReaderPtr StoragePool::newMetaReader(ReadLimiterPtr read_limiter, bool snapshot_read, const String & tracing_id)
{
    return newReader(
        run_mode,
        keyspace_id,
        StorageType::Meta,
        table_id,
        meta_storage_v2,
        meta_storage_v3,
        uni_ps,
        read_limiter,
        snapshot_read,
        tracing_id);
}

} // namespace DB::DM
