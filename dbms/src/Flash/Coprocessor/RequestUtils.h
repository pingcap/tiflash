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

#include <Common/Exception.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif
#include <pingcap/coprocessor/Client.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace DB::RequestUtils
{
template <typename PbRegion>
void setUpRegion(const pingcap::coprocessor::RegionInfo & region_info, PbRegion * region)
{
    region->set_region_id(region_info.region_id.id);
    region->mutable_region_epoch()->set_version(region_info.region_id.ver);
    region->mutable_region_epoch()->set_conf_ver(region_info.region_id.conf_ver);
    for (const auto & key_range : region_info.ranges)
    {
        key_range.setKeyRange(region->add_ranges());
    }
}

template <typename RequestPtr>
std::vector<pingcap::kv::RegionVerID> setUpRegionInfos(
    const pingcap::coprocessor::BatchCopTask & batch_cop_task,
    const RequestPtr & req)
{
    RUNTIME_CHECK_MSG(
        batch_cop_task.region_infos.empty() != batch_cop_task.table_regions.empty(),
        "region_infos and table_regions should not exist at the same time, single table region info: {}, partition "
        "table region info: {}",
        batch_cop_task.region_infos.size(),
        batch_cop_task.table_regions.size());

    std::vector<pingcap::kv::RegionVerID> region_ids;
    if (!batch_cop_task.region_infos.empty())
    {
        // For non-partition table
        region_ids.reserve(batch_cop_task.region_infos.size());
        for (const auto & region_info : batch_cop_task.region_infos)
        {
            region_ids.push_back(region_info.region_id);
            setUpRegion(region_info, req->add_regions());
        }
        return region_ids;
    }
    // For partition table
    for (const auto & table_region : batch_cop_task.table_regions)
    {
        auto * req_table_region = req->add_table_regions();
        req_table_region->set_physical_table_id(table_region.physical_table_id);
        for (const auto & region_info : table_region.region_infos)
        {
            region_ids.push_back(region_info.region_id);
            setUpRegion(region_info, req_table_region->add_regions());
        }
    }
    return region_ids;
}

template <typename Context>
pingcap::pd::KeyspaceID deriveKeyspaceID(const Context & ctx)
{
    return ctx.api_version() == kvrpcpb::APIVersion::V1 ? pingcap::pd::NullspaceID
                                                        : static_cast<pingcap::pd::KeyspaceID>(ctx.keyspace_id());
}

} // namespace DB::RequestUtils
