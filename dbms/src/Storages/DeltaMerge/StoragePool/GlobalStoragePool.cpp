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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/StoragePool/GlobalStoragePool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePoolConfig.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/PathPool.h>

namespace DB::DM
{

GlobalStoragePool::GlobalStoragePool(const PathPool & path_pool, Context & global_ctx, const Settings & settings)
    : log_storage(PageStorage::create(
        "__global__.log",
        path_pool.getPSDiskDelegatorGlobalMulti(PathPool::log_path_prefix),
        extractConfig(settings, StorageType::Log),
        global_ctx.getFileProvider(),
        global_ctx,
        true))
    , data_storage(PageStorage::create(
          "__global__.data",
          path_pool.getPSDiskDelegatorGlobalMulti(PathPool::data_path_prefix),
          extractConfig(settings, StorageType::Data),
          global_ctx.getFileProvider(),
          global_ctx,
          true))
    , meta_storage(PageStorage::create(
          "__global__.meta",
          path_pool.getPSDiskDelegatorGlobalMulti(PathPool::meta_path_prefix),
          extractConfig(settings, StorageType::Meta),
          global_ctx.getFileProvider(),
          global_ctx,
          true))
    , global_context(global_ctx)
{}


GlobalStoragePool::~GlobalStoragePool()
{
    shutdown();
}

void GlobalStoragePool::restore()
{
    log_storage->restore();
    data_storage->restore();
    meta_storage->restore();

    gc_handle = global_context.getBackgroundPool().addTask(
        [this] { return this->gc(global_context.getSettingsRef()); },
        false);
}

void GlobalStoragePool::shutdown()
{
    if (gc_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        gc_handle = {};
    }
}

FileUsageStatistics GlobalStoragePool::getLogFileUsage() const
{
    return log_storage->getFileUsageStatistics();
}

bool GlobalStoragePool::gc()
{
    return gc(global_context.getSettingsRef(), /*immediately=*/true, DELTA_MERGE_GC_PERIOD);
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

} // namespace DB::DM
