#include <DataStreams/StringStreamBlockInputStream.h>
#include <Debug/ClusterManage.h>
#include <Debug/DBGInvoker.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Debug/dbgFuncFailPoint.h>
#include <Debug/dbgFuncMisc.h>
#include <Debug/dbgFuncMockRaftCommand.h>
#include <Debug/dbgFuncMockTiDBData.h>
#include <Debug/dbgFuncMockTiDBTable.h>
#include <Debug/dbgFuncRegion.h>
#include <Debug/dbgFuncSchema.h>
#include <Debug/dbgFuncSchemaName.h>
#include <Parsers/ASTLiteral.h>

#include <cstring>
#include <thread>

namespace DB
{

void dbgFuncEcho(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    for (auto it = args.begin(); it != args.end(); ++it)
        output((*it)->getColumnName());
}

void dbgFuncSleep(Context &, const ASTs & args, DBGInvoker::Printer output)
{
    const Int64 t = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    std::this_thread::sleep_for(std::chrono::milliseconds(t));
    std::stringstream res;
    res << "sleep " << t << " ms.";
    output(res.str());
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

    regSchemalessFunc("set_flush_threshold", dbgFuncSetFlushThreshold);

    regSchemalessFunc("raft_insert_row", dbgFuncRaftInsertRow);
    regSchemalessFunc("raft_insert_row_full", dbgFuncRaftInsertRowFull);
    regSchemalessFunc("raft_insert_rows", dbgFuncRaftInsertRows);
    regSchemalessFunc("raft_update_rows", dbgFuncRaftUpdateRows);
    regSchemalessFunc("raft_delete_rows", dbgFuncRaftDelRows);
    regSchemalessFunc("raft_delete_row", dbgFuncRaftDeleteRow);

    regSchemalessFunc("put_region", dbgFuncPutRegion);

    regSchemalessFunc("try_flush", dbgFuncTryFlush);
    regSchemalessFunc("try_flush_region", dbgFuncTryFlushRegion);

    regSchemalessFunc("dump_all_region", dbgFuncDumpAllRegion);
    regSchemalessFunc("dump_all_mock_region", dbgFuncDumpAllMockRegion);
    regSchemalessFunc("remove_region", dbgFuncRemoveRegion);
    regSchemalessFunc("find_region_by_range", ClusterManage::findRegionByRange);
    regSchemalessFunc("check_table_optimize", ClusterManage::checkTableOptimize);

    regSchemalessFunc("enable_schema_sync_service", dbgFuncEnableSchemaSyncService);
    regSchemalessFunc("refresh_schemas", dbgFuncRefreshSchemas);
    regSchemalessFunc("gc_schemas", dbgFuncGcSchemas);
    regSchemalessFunc("reset_schemas", dbgFuncResetSchemas);
    regSchemalessFunc("is_tombstone", dbgFuncIsTombstone);

    regSchemalessFunc("region_split", MockRaftCommand::dbgFuncRegionBatchSplit);
    regSchemalessFunc("region_prepare_merge", MockRaftCommand::dbgFuncPrepareMerge);
    regSchemalessFunc("region_commit_merge", MockRaftCommand::dbgFuncCommitMerge);
    regSchemalessFunc("region_rollback_merge", MockRaftCommand::dbgFuncRollbackMerge);

    regSchemalessFunc("region_snapshot", MockRaftCommand::dbgFuncRegionSnapshot);
    regSchemalessFunc("region_snapshot_data", MockRaftCommand::dbgFuncRegionSnapshotWithData);
    regSchemalessFunc("region_snapshot_pre_handle_block", /**/ MockRaftCommand::dbgFuncRegionSnapshotPreHandleBlock);
    regSchemalessFunc("region_snapshot_apply_block", /*     */ MockRaftCommand::dbgFuncRegionSnapshotApplyBlock);
    regSchemalessFunc("region_snapshot_pre_handle_file", /* */ MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFiles);
    regSchemalessFunc("region_snapshot_pre_handle_file_pks", MockRaftCommand::dbgFuncRegionSnapshotPreHandleDTFilesWithHandles);
    regSchemalessFunc("region_snapshot_apply_file", /*      */ MockRaftCommand::dbgFuncRegionSnapshotApplyDTFiles);
    regSchemalessFunc("region_ingest_sst", MockRaftCommand::dbgFuncIngestSST);

    regSchemalessFunc("init_fail_point", DbgFailPointFunc::dbgInitFailPoint);
    regSchemalessFunc("enable_fail_point", DbgFailPointFunc::dbgEnableFailPoint);
    regSchemalessFunc("disable_fail_point", DbgFailPointFunc::dbgDisableFailPoint);
    regSchemalessFunc("wait_fail_point", DbgFailPointFunc::dbgDisableFailPoint);

    regSchemafulFunc("dag", dbgFuncTiDBQuery);
    regSchemafulFunc("mock_dag", dbgFuncMockTiDBQuery);
    regSchemafulFunc("tidb_query", dbgFuncTiDBQuery);
    regSchemafulFunc("tidb_mock_query", dbgFuncMockTiDBQuery);

    regSchemalessFunc("mapped_database", dbgFuncMappedDatabase);
    regSchemalessFunc("mapped_table", dbgFuncMappedTable);
    regSchemafulFunc("query_mapped", dbgFuncQueryMapped);

    regSchemalessFunc("search_log_for_key", dbgFuncSearchLogForKey);
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
    Context & context, const std::string & name, const SchemalessDBGFunc & func, const ASTs & args)
{
    std::stringstream col_name;
    col_name << name << "(";
    for (size_t i = 0; i < args.size(); ++i)
    {
        std::string arg = args[i]->getColumnName();
        col_name << normalizeArg(arg) << ((i + 1 == args.size()) ? "" : ", ");
    }
    col_name << ")";

    std::shared_ptr<StringStreamBlockInputStream> res = std::make_shared<StringStreamBlockInputStream>(col_name.str());
    Printer printer = [&](const std::string & s) { res->append(s); };

    func(context, args, printer);

    return res;
}

BlockInputStreamPtr DBGInvoker::invokeSchemaful(Context & context, const std::string &, const SchemafulDBGFunc & func, const ASTs & args)
{
    return func(context, args);
}

} // namespace DB
