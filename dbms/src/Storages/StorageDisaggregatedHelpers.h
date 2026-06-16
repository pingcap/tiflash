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

#include <Common/Exception.h>
#include <Common/RedactHelpers.h>
#include <Storages/KVStore/Types.h>
#include <kvproto/coprocessor.pb.h>
#include <kvproto/disaggregated.pb.h>
#include <kvproto/kvrpcpb.pb.h>
#include <pingcap/kv/LockResolver.h>

#include <memory>
#include <unordered_set>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

// Convert lock errors from disaggregated read responses into client-c locks.
// This boundary intentionally rejects SharedLock wrappers: normal TiFlash read
// paths should treat them as read-compatible and skip them before reporting a
// lock conflict, so resolving one here would hide an upstream invariant break.
inline pingcap::kv::LockPtr makeLockForDisaggResolve(const kvrpcpb::LockInfo & lock_info)
{
    if (lock_info.lock_type() == kvrpcpb::Op::SharedLock)
    {
        // TiFlash read paths must skip shared locks before reporting a lock conflict.
        // Seeing a SharedLock here means an upstream read path violated that invariant.
        // Do not resolve the wrapper: its txn fields are intentionally invalid.
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected SharedLock in TiFlash disaggregated lock resolver; shared locks are read-compatible and should "
            "be skipped before producing locked errors, lock_key={}, lock_version={}, lock_ttl={}, min_commit_ts={}, "
            "lock_for_update_ts={}, txn_size={}, shared_lock_count={}",
            Redact::keyToDebugString(lock_info.key().data(), lock_info.key().size()),
            lock_info.lock_version(),
            lock_info.lock_ttl(),
            lock_info.min_commit_ts(),
            lock_info.lock_for_update_ts(),
            lock_info.txn_size(),
            lock_info.shared_lock_infos_size());
    }
    return std::make_shared<pingcap::kv::Lock>(lock_info);
}

inline std::vector<pingcap::kv::LockPtr> makeLocksForDisaggResolve(
    const google::protobuf::RepeatedPtrField<kvrpcpb::LockInfo> & lock_infos)
{
    std::vector<pingcap::kv::LockPtr> locks;
    locks.reserve(lock_infos.size());
    for (const auto & lock_info : lock_infos)
        locks.emplace_back(makeLockForDisaggResolve(lock_info));
    return locks;
}

template <typename RegionCachePtr>
void dropRegionCache(
    const RegionCachePtr & region_cache,
    const std::shared_ptr<disaggregated::EstablishDisaggTaskRequest> & req,
    std::unordered_set<RegionID> && retry_regions)
{
    std::unordered_set<RegionID> dropped_regions;
    auto retry_from_region_infos
        = [&retry_regions, &region_cache, &dropped_regions](
              const google::protobuf::RepeatedPtrField<::coprocessor::RegionInfo> & region_infos) {
              for (const auto & region : region_infos)
              {
                  if (retry_regions.contains(region.region_id()))
                  {
                      auto region_ver_id = pingcap::kv::RegionVerID(
                          region.region_id(),
                          region.region_epoch().conf_ver(),
                          region.region_epoch().version());
                      region_cache->dropRegion(region_ver_id);
                      // There could be multiple Region info in `region_cache` share the same RegionID but with different epoch,
                      // we need to drop all their cache.
                      dropped_regions.insert(region.region_id());
                      if (retry_regions.empty())
                          break;
                  }
              }
          };

    // non-partition table
    retry_from_region_infos(req->regions());
    // partition table
    for (const auto & table_region : req->table_regions())
    {
        if (retry_regions.empty())
            break;
        retry_from_region_infos(table_region.regions());
    }

    if (unlikely(dropped_regions.size() < retry_regions.size()))
    {
        for (const auto & id : dropped_regions)
            retry_regions.erase(id);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to drop regions {} from the cache", retry_regions);
    }
}
} // namespace DB
