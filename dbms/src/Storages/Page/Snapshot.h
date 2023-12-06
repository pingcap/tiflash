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
#include <Common/assert_cast.h>
#include <common/types.h>

#include <memory>

namespace DB
{
struct SnapshotsStatistics
{
    size_t num_snapshots = 0;
    double longest_living_seconds = 0.0;
    unsigned longest_living_from_thread_id = 0;
    String longest_living_from_tracing_id;
};
class PageStorageSnapshot
{
public:
    virtual ~PageStorageSnapshot() = default;
};
using PageStorageSnapshotPtr = std::shared_ptr<PageStorageSnapshot>;

class PageStorageSnapshotMixed : public PageStorageSnapshot
{
public:
    // TODO: add/sub CurrentMetrics::PSMVCCNumSnapshots in here
    PageStorageSnapshotMixed(const PageStorageSnapshotPtr & snapshot_v2_, const PageStorageSnapshotPtr & snapshot_v3_)
        : snapshot_v2(snapshot_v2_)
        , snapshot_v3(snapshot_v3_)
    {}

    ~PageStorageSnapshotMixed() override = default;

    PageStorageSnapshotPtr getV2Snapshot() { return snapshot_v2; }

    PageStorageSnapshotPtr getV3Snapshot() { return snapshot_v3; }

private:
    PageStorageSnapshotPtr snapshot_v2;
    PageStorageSnapshotPtr snapshot_v3;
};
using PageStorageSnapshotMixedPtr = std::shared_ptr<PageStorageSnapshotMixed>;

inline PageStorageSnapshotMixedPtr toConcreteMixedSnapshot(const PageStorageSnapshotPtr & ptr)
{
    return std::static_pointer_cast<PageStorageSnapshotMixed>(ptr);
}

} // namespace DB
