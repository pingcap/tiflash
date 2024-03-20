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

#include <Common/FmtUtils.h>
#include <Common/typeid_cast.h>
#include <DataStreams/StringStreamBlockInputStream.h>
#include <Debug/DBGInvoker.h>
#include <Debug/ReadIndexStressTest.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Debug/dbgFuncFailPoint.h>
#include <Debug/dbgFuncMisc.h>
#include <Debug/dbgFuncMockTiDBData.h>
#include <Debug/dbgFuncMockTiDBTable.h>
#include <Debug/dbgFuncSchema.h>
#include <Debug/dbgFuncSchemaName.h>
#include <Debug/dbgKVStore/dbgFuncInvestigator.h>
#include <Debug/dbgKVStore/dbgFuncMockRaftCommand.h>
#include <Debug/dbgKVStore/dbgFuncRegion.h>
#include <Parsers/ASTLiteral.h>

#include <thread>

namespace DB
{
void dbgFuncEcho(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    for (const auto & arg : args)
        output(arg->getColumnName());
}

void dbgFuncSleep(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    const Int64 t = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    std::this_thread::sleep_for(std::chrono::milliseconds(t));
    output(fmt::format("sleep {} ms.", t));
}

DBGInvoker::DBGInvoker()
{
    regSchemalessFunc("echo", dbgFuncEcho);
    // TODO: remove this, use sleep in bash script
    regSchemalessFunc("sleep", dbgFuncSleep);

    regSchemalessFunc("clean_up_region", MockTiDBTable::dbgFuncCleanUpRegions);
    regSchemalessFunc("mock_tidb_table", MockTiDBTable::dbgFuncMockTiDBTable);
    regSchemalessFunc("mock_tidb_db", MockTiDBTable::dbgFuncMockTiDBDB);
    regSchemalessFunc("mock_tidb_partition", MockTiDBTable::dbgFuncMockTiDBPartition);
    regSchemalessFunc("drop_tidb_table", MockTiDBTable::dbgFuncDropTiDBTable);
    regSchemalessFunc("drop_tidb_db", MockTiDBTable::dbgFuncDropTiDBDB);
    regSchemalessFunc("drop_tidb_partition", MockTiDBTable::dbgFuncDropTiDBPartition);
    regSchemalessFunc("add_column_to_tidb_table", MockTiDBTable::dbgFuncAddColumnToTiDBTable);
    regSchemalessFunc("drop_column_from_tidb_table", MockTiDBTable::dbgFuncDropColumnFromTiDBTable);
    regSchemalessFunc("modify_column_in_tidb_table", MockTiDBTable::dbgFuncModifyColumnInTiDBTable);
    regSchemalessFunc("rename_column_in_tidb_table", MockTiDBTable::dbgFuncRenameColumnInTiDBTable);
    regSchemalessFunc("rename_tidb_table", MockTiDBTable::dbgFuncRenameTiDBTable);
    regSchemalessFunc("truncate_tidb_table", MockTiDBTable::dbgFuncTruncateTiDBTable);
    regSchemalessFunc("create_tidb_tables", MockTiDBTable::dbgFuncCreateTiDBTables);
    regSchemalessFunc("rename_tidb_tables", MockTiDBTable::dbgFuncRenameTiDBTables);

    regSchemalessFunc("raft_insert_row", dbgFuncRaftInsertRow);
    regSchemalessFunc("raft_insert_row_full", dbgFuncRaftInsertRowFull);
    regSchemalessFunc("raft_insert_rows", dbgFuncRaftInsertRows);
    regSchemalessFunc("raft_update_rows", dbgFuncRaftUpdateRows);
    regSchemalessFunc("raft_delete_rows", dbgFuncRaftDelRows);
    regSchemalessFunc("raft_delete_row", dbgFuncRaftDeleteRow);

    regSchemalessFunc("put_region", dbgFuncPutRegion);

    regSchemalessFunc("try_flush_region", dbgFuncTryFlushRegion);

    regSchemalessFunc("dump_all_region", dbgFuncDumpAllRegion);
    regSchemalessFunc("dump_all_mock_region", dbgFuncDumpAllMockRegion);
    regSchemalessFunc("remove_region", dbgFuncRemoveRegion);
    regSchemalessFunc("find_region_by_range", dbgFuncFindRegionByRange);

    regSchemalessFunc("enable_schema_sync_service", dbgFuncEnableSchemaSyncService);
    regSchemalessFunc("refresh_schemas", dbgFuncRefreshSchemas);
    regSchemalessFunc("gc_schemas", dbgFuncGcSchemas);
    regSchemalessFunc("reset_schemas", dbgFuncResetSchemas);
    regSchemalessFunc("is_tombstone", dbgFuncIsTombstone);
    regSchemalessFunc("refresh_table_schema", dbgFuncRefreshTableSchema);
    regSchemalessFunc("refresh_mapped_table_schema", dbgFuncRefreshMappedTableSchema);
    regSchemalessFunc("skip_schema_version", dbgFuncSkipSchemaVersion);
    regSchemalessFunc("regenerate_schema_map", dbgFuncRegrenationSchemaMap);

    regSchemalessFunc("region_split", MockRaftCommand::dbgFuncRegionBatchSplit);
    regSchemalessFunc("region_prepare_merge", MockRaftCommand::dbgFuncPrepareMerge);
    regSchemalessFunc("region_commit_merge", MockRaftCommand::dbgFuncCommitMerge);
    regSchemalessFunc("region_rollback_merge", MockRaftCommand::dbgFuncRollbackMerge);

    regSchemalessFunc("region_snapshot", MockRaftCommand::dbgFuncRegionSnapshot);
    regSchemalessFunc("region_snapshot_data", MockRaftCommand::dbgFuncRegionSnapshotWithData);
    regSchemalessFunc("region_snapshot_pre_handle_block", /**/ MockRaftCommand::dbgFuncRegionSnapshotPreHandleBlock);
    regSchemalessFunc("region_snapshot_apply_block", /*     */ MockRaftCommand::dbgFuncRegionSnapshotApplyBlock);
    regSchemalessFunc("region_snapshot_pre_handle_file", /* */ MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFiles);
    regSchemalessFunc(
        "region_snapshot_pre_handle_file_pks",
        MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFilesWithHandles);
    regSchemalessFunc("region_snapshot_apply_file", /*      */ MockRaftCommand::dbgFuncRegionSnapshotApplyDTFiles);
    regSchemalessFunc("region_ingest_sst", MockRaftCommand::dbgFuncIngestSST);
    regSchemalessFunc("find_key", dbgFuncFindKey);

    regSchemalessFunc("init_fail_point", DbgFailPointFunc::dbgInitFailPoint);
    regSchemalessFunc("enable_fail_point", DbgFailPointFunc::dbgEnableFailPoint);
    regSchemalessFunc("enable_pause_fail_point", DbgFailPointFunc::dbgEnablePauseFailPoint);
    regSchemalessFunc("disable_fail_point", DbgFailPointFunc::dbgDisableFailPoint);
    regSchemalessFunc("wait_fail_point", DbgFailPointFunc::dbgDisableFailPoint);

    regSchemafulFunc("dag", dbgFuncTiDBQuery);
    regSchemafulFunc("mock_dag", dbgFuncMockTiDBQuery);
    regSchemafulFunc("tidb_query", dbgFuncTiDBQuery);
    regSchemafulFunc("tidb_mock_query", dbgFuncMockTiDBQuery);

    regSchemalessFunc("mapped_database", dbgFuncMappedDatabase);
    regSchemalessFunc("mapped_table", dbgFuncMappedTable);
    regSchemalessFunc("mapped_table_exists", dbgFuncTableExists);
    regSchemalessFunc("mapped_database_exists", dbgFuncDatabaseExists);
    regSchemafulFunc("query_mapped", dbgFuncQueryMapped);
    regSchemalessFunc("get_tiflash_replica_count", dbgFuncGetTiflashReplicaCount);
    regSchemalessFunc("get_partition_tables_tiflash_replica_count", dbgFuncGetPartitionTablesTiflashReplicaCount);

    regSchemalessFunc("search_log_for_key", dbgFuncSearchLogForKey);
    regSchemalessFunc("tidb_dag", dbgFuncTiDBQueryFromNaturalDag);
    regSchemalessFunc("gc_global_storage_pool", dbgFuncTriggerGlobalPageStorageGC);

    regSchemalessFunc("read_index_stress_test", ReadIndexStressTest::dbgFuncStressTest);

    regSchemalessFunc(
        "wait_until_no_temp_active_threads_in_dynamic_thread_pool",
        dbgFuncWaitUntilNoTempActiveThreadsInDynamicThreadPool);
}

void replaceSubstr(std::string & str, const std::string & target, const std::string & replacement)
{
    while (true)
    {
        auto found = str.find(target);
        if (found == std::string::npos)
            break;
        str.replace(found, target.size(), replacement);
    }
}

std::string & normalizeArg(std::string & arg)
{
    replaceSubstr(arg, "\'", "\"");
    return arg;
}

BlockInputStreamPtr DBGInvoker::invoke(Context & context, const std::string & ori_name, const ASTs & args)
{
    static const std::string prefix_not_print_res = "__";
    bool print_res = true;
    std::string name = ori_name;
    if (ori_name.size() > prefix_not_print_res.size()
        && std::memcmp(ori_name.data(), prefix_not_print_res.data(), prefix_not_print_res.size()) == 0)
    {
        print_res = false;
        name = ori_name.substr(prefix_not_print_res.size(), ori_name.size() - prefix_not_print_res.size());
    }

    BlockInputStreamPtr res;
    auto it_schemaless = schemaless_funcs.find(name);
    if (it_schemaless != schemaless_funcs.end())
        res = invokeSchemaless(context, name, it_schemaless->second, args);
    else
    {
        auto it_schemaful = schemaful_funcs.find(name);
        if (it_schemaful != schemaful_funcs.end())
            res = invokeSchemaful(context, name, it_schemaful->second, args);
        else
            throw Exception("DBG function not found", ErrorCodes::BAD_ARGUMENTS);
    }

    return print_res ? res : std::shared_ptr<StringStreamBlockInputStream>();
}

BlockInputStreamPtr DBGInvoker::invokeSchemaless(
    Context & context,
    const std::string & name,
    const SchemalessDBGFunc & func,
    const ASTs & args)
{
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("{}(", name);
    fmt_buf.joinStr(
        args.cbegin(),
        args.cend(),
        [](const auto & arg, FmtBuffer & fb) {
            std::string column_name = arg->getColumnName();
            fb.append(normalizeArg(column_name));
        },
        ", ");
    fmt_buf.append(")");

    std::shared_ptr<StringStreamBlockInputStream> res
        = std::make_shared<StringStreamBlockInputStream>(fmt_buf.toString());
    Printer printer = [&](const std::string & s) {
        res->append(s);
    };

    func(context, args, printer);

    return res;
}

BlockInputStreamPtr DBGInvoker::invokeSchemaful(
    Context & context,
    const std::string &,
    const SchemafulDBGFunc & func,
    const ASTs & args)
{
    return func(context, args);
}

} // namespace DB
