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
#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/RegionPersister.h>

#include <optional>

namespace DB
{
namespace tests
{
#define ASSERT_CHECK(cond, res)                                  \
    do                                                           \
    {                                                            \
        if (!(cond))                                             \
        {                                                        \
            std::cerr << __FILE__ << ":" << __LINE__ << ":"      \
                      << " Assertion " << #cond << " failed.\n"; \
            if ((res))                                           \
            {                                                    \
                (res) = false;                                   \
            }                                                    \
        }                                                        \
    } while (0)

#define ASSERT_CHECK_EQUAL(a, b, res)                                         \
    do                                                                        \
    {                                                                         \
        if (!((a) == (b)))                                                    \
        {                                                                     \
            std::cerr << __FILE__ << ":" << __LINE__ << ":"                   \
                      << " Assertion " << #a << " == " << #b << " failed.\n"; \
            if ((res))                                                        \
            {                                                                 \
                (res) = false;                                                \
            }                                                                 \
        }                                                                     \
    } while (0)


inline metapb::Peer createPeer(UInt64 id, bool)
{
    metapb::Peer peer;
    peer.set_id(id);
    return peer;
}

inline metapb::Region createRegionInfo(UInt64 id, const std::string start_key, const std::string end_key, std::optional<metapb::RegionEpoch> maybe_epoch = std::nullopt, std::optional<std::vector<metapb::Peer>> maybe_peers = std::nullopt)
{
    metapb::Region region_info;
    region_info.set_id(id);
    region_info.set_start_key(start_key);
    region_info.set_end_key(end_key);
    if (maybe_epoch)
    {
        *region_info.mutable_region_epoch() = (maybe_epoch.value());
    }
    else
    {
        region_info.mutable_region_epoch()->set_version(5);
        region_info.mutable_region_epoch()->set_version(6);
    }
    if (maybe_peers)
    {
        auto & peers = maybe_peers.value();
        for (auto it = peers.begin(); it != peers.end(); it++)
        {
            *(region_info.mutable_peers()->Add()) = *it;
        }
    }
    else
    {
        *(region_info.mutable_peers()->Add()) = createPeer(1, true);
        *(region_info.mutable_peers()->Add()) = createPeer(2, false);
    }

    return region_info;
}

inline RegionMeta createRegionMeta(UInt64 id, DB::TableID table_id, std::optional<raft_serverpb::RaftApplyState> apply_state = std::nullopt)
{
    return RegionMeta(/*peer=*/createPeer(31, true),
                      /*region=*/createRegionInfo(id, RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 300)),
                      /*apply_state_=*/apply_state.value_or(initialApplyState()));
}

inline RegionPtr makeRegion(UInt64 id, const std::string start_key, const std::string end_key, const TiFlashRaftProxyHelper * proxy_helper = nullptr)
{
    return std::make_shared<Region>(
        RegionMeta(
            createPeer(2, true),
            createRegionInfo(id, std::move(start_key), std::move(end_key)),
            initialApplyState()),
        proxy_helper);
}

} // namespace tests
} // namespace DB
