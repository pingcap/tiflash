// Copyright 2025 PingCAP, Inc.
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

#include <Common/TiFlashMetrics.h>
#include <Storages/KVStore/Read/RegionException.h>
#include <kvproto/coprocessor.pb.h>
#include <kvproto/errorpb.pb.h>

namespace DB
{
void setResponseByRegionException(coprocessor::Response * response, const RegionException & e, RegionID region_id)
{
    assert(response != nullptr);

    errorpb::Error * region_err;
    switch (e.status)
    {
    case RegionException::RegionReadStatus::OK:
    case RegionException::RegionReadStatus::MEET_LOCK:
        // Should not happen RegionException with RegionReadStatus::OK status
        // And RegionReadStatus::MEET_LOCK should be handled in LockException
        region_err = response->mutable_region_error();
        region_err->set_message(
            fmt::format("Unexpected RegionException with status {}", magic_enum::enum_name(e.status)));
        region_err->mutable_region_not_found()->set_region_id(region_id);
        break;
    case RegionException::RegionReadStatus::OTHER:
    case RegionException::RegionReadStatus::BUCKET_EPOCH_NOT_MATCH:
    case RegionException::RegionReadStatus::FLASHBACK:
    case RegionException::RegionReadStatus::KEY_NOT_IN_REGION:
    case RegionException::RegionReadStatus::TIKV_SERVER_ISSUE:
    case RegionException::RegionReadStatus::READ_INDEX_TIMEOUT:
    case RegionException::RegionReadStatus::NOT_LEADER:
    case RegionException::RegionReadStatus::NOT_FOUND_TIKV:
    case RegionException::RegionReadStatus::NOT_FOUND:
    case RegionException::RegionReadStatus::STALE_COMMAND:
    case RegionException::RegionReadStatus::STORE_NOT_MATCH:
        GET_METRIC(tiflash_coprocessor_request_error, reason_region_not_found).Increment();
        region_err = response->mutable_region_error();
        region_err->mutable_region_not_found()->set_region_id(region_id);
        break;
    case RegionException::RegionReadStatus::EPOCH_NOT_MATCH:
        GET_METRIC(tiflash_coprocessor_request_error, reason_epoch_not_match).Increment();
        region_err = response->mutable_region_error();
        region_err->mutable_epoch_not_match();
        // TODO: set current_regions when mutable epoch not match is hit
        break;
        // Notice: We need to handle all enum cases here to ensure correctly retrying.
        // Do NOT add a default case here.
        // default:
        //     break;
    }
}
} // namespace DB
