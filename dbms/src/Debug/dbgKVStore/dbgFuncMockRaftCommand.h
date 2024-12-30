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

#include <Debug/DBGInvoker.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;

struct MockRaftCommand
{
    // split region
    // Usage:
    //   ./storages-client.sh "DBGInvoke region_split(region_id1, database_name, table_name, start1, end1, start2, end2, region_id2)"
    static void dbgFuncRegionBatchSplit(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // region prepare merge
    // Usage:
    //   ./storages-client.sh "DBGInvoke region_prepare_merge(region_id, database_name, table_name)"
    static void dbgFuncPrepareMerge(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // region prepare merge
    // Usage:
    //   ./storages-client.sh "DBGInvoke region_commit_merge(region_id, database_name, table_name, start1, end1, start2, end2)"
    static void dbgFuncCommitMerge(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // region prepare merge
    // Usage:
    //   ./storages-client.sh "DBGInvoke region_rollback_merge(region_id, database_name, table_name, start1, end1, start2, end2)"
    static void dbgFuncRollbackMerge(Context & context, const ASTs & args, DBGInvoker::Printer output);


    /// Mock apply snapshot / ingest sst

    // Simulate a region snapshot raft command
    // Usage:
    //   ./storages-client.sh "DBGInvoke region_snapshot(region_id, start, end, database_name, table_name[, partition_id])"
    static void dbgFuncRegionSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Simulate a region snapshot raft command
    // Usage:
    //   ./storages-client.sh "DBGInvoke region_snapshot_data(database_name, table_name, region_id, start, end, handle_id1, tso1, del1, r1_c1, r1_c2, ..., handle_id2, tso2, del2, r2_c1, r2_c2, ... )"
    static void dbgFuncRegionSnapshotWithData(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Simulate a region IngestSST raft command
    // Usage:
    //    ./storage-client.sh "DBGInvoke region_ingest_sst(database_name, table_name, region_id, start, end)"
    static void dbgFuncIngestSST(Context & context, const ASTs & args, DBGInvoker::Printer output);

    // Simulate a region pre-handle snapshot data to DTFiles
    // Usage:
    //    ./storage-client.sh "DBGInvoke region_snapshot_pre_handle_file(database_name, table_name, region_id, start, end, schema_string, pk_name[, test-fields=1, cfs="write,default"])"
    static void dbgFuncRegionSnapshotPreHandleDTFiles(Context & context, const ASTs & args, DBGInvoker::Printer output);

    static void dbgFuncRegionSnapshotPreHandleDTFilesWithHandles(
        Context & context,
        const ASTs & args,
        DBGInvoker::Printer output);

    // Apply snapshot for a region. (apply a pre-handle snapshot)
    // Usage:
    //   ./storages-client.sh "DBGInvoke region_snapshot_apply_file(region_id)"
    static void dbgFuncRegionSnapshotApplyDTFiles(Context & context, const ASTs & args, DBGInvoker::Printer output);
};

} // namespace DB
