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
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Storages/Transaction/LearnerRead.h>

#include <unordered_map>

namespace DB
{
class Context;
class TMTContext;

using TablesRegionInfoMap = std::unordered_map<Int64, std::reference_wrapper<const RegionInfoMap>>;

class TiDBReadSnapshot
{
public:
    TiDBReadSnapshot(
        Context & context_,
        const TiDBTableScan & tidb_table_scan_,
        const String & req_id);

    void validateQueryInfo(const MvccQueryInfo & mvcc_query_info);

    void releaseLearnerReadSnapshot();

    RegionRetryList moveRegionRetryFromLocalRegion() { return std::move(region_retry_from_local_region); }

    std::unique_ptr<MvccQueryInfo> moveMvccQueryInfo() { return std::move(mvcc_query_info); }

    ~TiDBReadSnapshot()
    {
        RUNTIME_ASSERT(snapshot_status == SnapshotStatus::released, log, "snapshot_status must be released status.");
    }

private:
    LearnerReadSnapshot doCopLearnerRead();
    LearnerReadSnapshot doBatchCopLearnerRead();

private:
    enum SnapshotStatus
    {
        read,
        validated,
        released,
    };

    Context & context;
    TMTContext & tmt;
    const TiDBTableScan & tidb_table_scan;

    LoggerPtr log;

    /// region_retry_from_local_region and mvcc_query_info will be moved.
    /// region_retry_from_local_region shouldn't be hash map because duplicated region id may occur if merge regions to retry of dag.
    RegionRetryList region_retry_from_local_region;
    std::unique_ptr<MvccQueryInfo> mvcc_query_info;

    // We need to validate regions snapshot after getting streams from storage.
    SnapshotStatus snapshot_status = SnapshotStatus::read;
    LearnerReadSnapshot learner_read_snapshot;
};
} // namespace DB