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

#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool_fwd.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageStorage_fwd.h>

#include <atomic>
#include <chrono>

namespace DB
{
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

struct Settings;
class PathPool;
class StableDiskDelegator;
class AsynchronousMetrics;
} // namespace DB

namespace DB::DM
{

class GlobalStoragePool : private boost::noncopyable
{
public:
    using Clock = std::chrono::system_clock;
    using Timepoint = Clock::time_point;
    using Seconds = std::chrono::seconds;

    GlobalStoragePool(const PathPool & path_pool, Context & global_ctx, const Settings & settings);

    ~GlobalStoragePool();

    void restore();

    void shutdown();

    friend class StoragePool;
    friend class ::DB::AsynchronousMetrics;

    // GC immediately
    // Only used on dbgFuncMisc
    bool gc();

    FileUsageStatistics getLogFileUsage() const;

private:
    bool gc(const Settings & settings, bool immediately = false, const Seconds & try_gc_period = DELTA_MERGE_GC_PERIOD);

private:
    PageStoragePtr log_storage;
    PageStoragePtr data_storage;
    PageStoragePtr meta_storage;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();

    Context & global_context;
    BackgroundProcessingPool::TaskHandle gc_handle;
};

} // namespace DB::DM
