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

#include <Common/Logger.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/WriteBatchImpl.h>

namespace DB
{
class Context;

class PathPool;
class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionTaskLock;
struct RegionManager;

struct TiFlashRaftProxyHelper;

class RegionPersister final : private boost::noncopyable
{
public:
    RegionPersister(Context & global_context_, const RegionManager & region_manager_);

    void drop(RegionID region_id, const RegionTaskLock &);
    void persist(const Region & region);
    void persist(const Region & region, const RegionTaskLock & lock);
    RegionMap restore(
        PathPool & path_pool,
        const TiFlashRaftProxyHelper * proxy_helper = nullptr,
        PageStorageConfig config = PageStorageConfig{});
    bool gc();

    using RegionCacheWriteElement = std::tuple<RegionID, MemoryWriteBuffer, size_t, UInt64>;
    static void computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer);
    static size_t computeRegionWriteBuffer(const Region & region, WriteBuffer & buffer);

    PageStorageConfig getPageStorageSettings() const;

    FileUsageStatistics getFileUsageStatistics() const;

private:
    void forceTransformKVStoreV2toV3();

    void doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock & lock, const Region & region);
    void doPersist(const Region & region, const RegionTaskLock * lock);

private:
    inline std::variant<String, NamespaceID> getWriteBatchPrefix() const
    {
        switch (run_mode)
        {
        case PageStorageRunMode::UNI_PS:
            return UniversalPageIdFormat::toFullPrefix(NullspaceID, StorageType::KVStore, ns_id);
        default:
            return ns_id;
        }
    }

private:
    Context & global_context;
    PageStorageRunMode run_mode;
    PageWriterPtr page_writer;
    PageReaderPtr page_reader;

    NamespaceID ns_id = KVSTORE_NAMESPACE_ID;
    const RegionManager & region_manager;
    std::mutex mutex;
    LoggerPtr log;
};
} // namespace DB
