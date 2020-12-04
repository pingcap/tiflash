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

    static void dbgFuncStorePreHandleSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output);

    static void dbgFuncApplyPreHandleSnapshot(Context & context, const ASTs & args, DBGInvoker::Printer output);
};

} // namespace DB
