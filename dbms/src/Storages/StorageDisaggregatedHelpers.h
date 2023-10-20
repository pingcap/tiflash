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
#include <Storages/KVStore/Types.h>
#include <kvproto/coprocessor.pb.h>
#include <kvproto/disaggregated.pb.h>

#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

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
