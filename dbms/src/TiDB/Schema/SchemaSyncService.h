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

#pragma once

#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Transaction/Types.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace DB
{
class Context;
class BackgroundProcessingPool;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;
using DBGInvokerPrinter = std::function<void(const std::string &)>;
extern void dbgFuncGcSchemas(Context &, const ASTs &, DBGInvokerPrinter);

namespace tests
{
class SchemaSyncTest;
}

class SchemaSyncService
    : public std::enable_shared_from_this<SchemaSyncService>
    , private boost::noncopyable
{
public:
    explicit SchemaSyncService(Context & context_);
    ~SchemaSyncService();

    friend class tests::SchemaSyncTest;
    bool gc(Timestamp gc_safepoint, KeyspaceID keyspace_id);

    void shutdown();

private:
    bool syncSchemas(KeyspaceID keyspace_id);
    void removeCurrentVersion(KeyspaceID keyspace_id);


    void addKeyspaceGCTasks();
    void removeKeyspaceGCTasks();

    std::optional<Timestamp> lastGcSafePoint(KeyspaceID keyspace_id) const;
    void updateLastGcSafepoint(KeyspaceID keyspace_id, Timestamp gc_safepoint);
    bool gcImpl(Timestamp gc_safepoint, KeyspaceID keyspace_id, bool ignore_remain_regions);

private:
    Context & context;

    friend void dbgFuncGcSchemas(Context &, const ASTs &, DBGInvokerPrinter);
    struct KeyspaceGCContext
    {
        Timestamp last_gc_safepoint = 0;
    };

    BackgroundProcessingPool & background_pool;
    BackgroundProcessingPool::TaskHandle handle;

    mutable std::shared_mutex ks_map_mutex;
    // Handles for each keyspace schema sync task.
    std::unordered_map<KeyspaceID, BackgroundProcessingPool::TaskHandle> ks_handle_map;
    std::unordered_map<KeyspaceID, KeyspaceGCContext> keyspace_gc_context;

    LoggerPtr log;
};

using SchemaSyncServicePtr = std::shared_ptr<SchemaSyncService>;

} // namespace DB
