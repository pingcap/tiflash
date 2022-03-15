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

#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

namespace DB
{
class Context;

class Region;
using RegionPtr = std::shared_ptr<Region>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RegionTaskLock;
struct RegionManager;

struct TiFlashRaftProxyHelper;
namespace PS
{
namespace V1
{
class PageStorage;
}
namespace V2
{
class PageStorage;
}
} // namespace PS

class RegionPersister final : private boost::noncopyable
{
public:
    RegionPersister(Context & global_context_, const RegionManager & region_manager_);

    void drop(RegionID region_id, const RegionTaskLock &);
    void persist(const Region & region);
    void persist(const Region & region, const RegionTaskLock & lock);
    RegionMap restore(const TiFlashRaftProxyHelper * proxy_helper = nullptr, PageStorage::Config config = PageStorage::Config{});
    bool gc();

    using RegionCacheWriteElement = std::tuple<RegionID, MemoryWriteBuffer, size_t, UInt64>;
    static void computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    void doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock & lock, const Region & region);
    void doPersist(const Region & region, const RegionTaskLock * lock);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    Context & global_context;
    std::shared_ptr<PS::V2::PageStorage> page_storage;
    std::shared_ptr<PS::V1::PageStorage> stable_page_storage;

    // RegionPersister stores it's data individually, so the `ns_id` value doesn't matter
    NamespaceId ns_id = MAX_NAMESPACE_ID;
    const RegionManager & region_manager;
    std::mutex mutex;
    Poco::Logger * log;
};
} // namespace DB
