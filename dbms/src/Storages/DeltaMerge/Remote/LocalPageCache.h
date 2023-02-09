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

#pragma once

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class Context;
class ReadBuffer;
using ReadBufferPtr = std::shared_ptr<ReadBuffer>;

class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;
} // namespace DB

namespace DB::DM::Remote
{

class LocalPageCache;
using LocalPageCachePtr = std::shared_ptr<LocalPageCache>;

class LocalPageCache : private boost::noncopyable
{
public:
    explicit LocalPageCache(const Context & global_context_);

    std::vector<PageOID> getMissingIds(const std::vector<PageOID> & pages);

    void write(
        const PageOID & oid,
        ReadBufferPtr && read_buffer,
        PageSize size,
        PageFieldSizes && field_sizes);

    Page getPage(const PageOID & oid, const PageStorage::FieldIndices & indices);

private:
    using LRUQueue = std::list<UniversalPageId>;
    using LRUQueueItr = typename LRUQueue::iterator;

    struct LRUElement
    {
        size_t size;
        LRUQueueItr iter;
    };

    const size_t lru_max_size; // 0 means LRU is disabled

    std::mutex lru_mu;
    LRUQueue lru_queue;
    std::unordered_map<UniversalPageId, LRUElement> lru_index; // TODO: Use ShardedLRU if necessary.
    size_t lru_current_size = 0;

    void lruPut(const UniversalPageId & key, size_t size);

    void lruRefresh(const UniversalPageId & key);

    std::vector<UniversalPageId> lruEvict();

    bool lruIsEnabled()
    {
        return lru_max_size > 0;
    }

private:
    LoggerPtr log;

    UniversalPageStoragePtr cache_storage;

    // TODO: Remove this when we introduce RPC. It is only used to access the write node PS.
    const Context & global_context;

    static UniversalPageId buildCacheId(const PageOID & oid)
    {
        return fmt::format("{}_{}_{}", oid.write_node_id, oid.table_id, oid.page_id);
    }
};

} // namespace DB::DM::Remote
