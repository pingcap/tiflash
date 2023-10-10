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

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Region.h>
#include <common/logger_useful.h>

namespace ProfileEvents
{
extern const Event RaftWaitIndexTimeout;
} // namespace ProfileEvents

namespace DB
{

kvrpcpb::ReadIndexRequest GenRegionReadIndexReq(const Region & region, UInt64 start_ts)
{
    auto meta_snap = region.dumpRegionMetaSnapshot();
    kvrpcpb::ReadIndexRequest request;
    {
        auto * context = request.mutable_context();
        context->set_region_id(region.id());
        *context->mutable_peer() = meta_snap.peer;
        context->mutable_region_epoch()->set_version(meta_snap.ver);
        context->mutable_region_epoch()->set_conf_ver(meta_snap.conf_ver);
        // if start_ts is 0, only send read index request to proxy
        if (start_ts)
        {
            request.set_start_ts(start_ts);
            auto * key_range = request.add_ranges();
            // use original tikv key
            key_range->set_start_key(meta_snap.range->comparableKeys().first.key);
            key_range->set_end_key(meta_snap.range->comparableKeys().second.key);
        }
    }
    return request;
}

bool Region::checkIndex(UInt64 index) const
{
    return meta.checkIndex(index);
}

std::tuple<WaitIndexResult, double> Region::waitIndex(
    UInt64 index,
    const UInt64 timeout_ms,
    std::function<bool(void)> && check_running)
{
    if (proxy_helper != nullptr)
    {
        if (!meta.checkIndex(index))
        {
            Stopwatch wait_index_watch;
            LOG_DEBUG(log, "{} need to wait learner index {} timeout {}", toString(), index, timeout_ms);
            auto wait_idx_res = meta.waitIndex(index, timeout_ms, std::move(check_running));
            auto elapsed_secs = wait_index_watch.elapsedSeconds();
            switch (wait_idx_res)
            {
            case WaitIndexResult::Finished:
            {
                LOG_DEBUG(log, "{} wait learner index {} done", toString(false), index);
                return {wait_idx_res, elapsed_secs};
            }
            case WaitIndexResult::Terminated:
            {
                return {wait_idx_res, elapsed_secs};
            }
            case WaitIndexResult::Timeout:
            {
                ProfileEvents::increment(ProfileEvents::RaftWaitIndexTimeout);
                LOG_WARNING(
                    log,
                    "{} wait learner index {} timeout current {} state {}",
                    toString(false),
                    index,
                    appliedIndex(),
                    peerState());
                return {wait_idx_res, elapsed_secs};
            }
            }
        }
    }
    return {WaitIndexResult::Finished, 0};
}

} // namespace DB