#include <DataStreams/StringStreamBlockInputStream.h>

#include <Debug/dbgFuncMockTiDBTable.h>
#include <Debug/dbgFuncMockTiDBData.h>
#include <Debug/dbgFuncRegion.h>

#include <Debug/DBGInvoker.h>
#include <cstring>
#include <thread>
#include <Parsers/ASTLiteral.h>

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
    regFunc("echo", dbgFuncEcho);
    // TODO: remove this, use sleep in bash script
    regFunc("sleep", dbgFuncSleep);

    regFunc("mock_schema_syncer", MockTiDBTable::dbgFuncMockSchemaSyncer);
    regFunc("mock_tidb_table", MockTiDBTable::dbgFuncMockTiDBTable);
    regFunc("mock_tidb_partition", MockTiDBTable::dbgFuncMockTiDBPartition);
    regFunc("rename_table_for_partition", MockTiDBTable::dbgFuncRenameTableForPartition);
    regFunc("drop_tidb_table", MockTiDBTable::dbgFuncDropTiDBTable);

    regFunc("set_flush_threshold", dbgFuncSetFlushThreshold);

    regFunc("raft_insert_row", dbgFuncRaftInsertRow);
    regFunc("raft_insert_rows", dbgFuncRaftInsertRows);
    regFunc("raft_update_rows", dbgFuncRaftUpdateRows);
    regFunc("raft_delete_rows", dbgFuncRaftDelRows);
    regFunc("raft_delete_row", dbgFuncRaftDeleteRow);

    regFunc("put_region", dbgFuncPutRegion);
    regFunc("region_snapshot", dbgFuncRegionSnapshot);
    regFunc("rm_region_data", dbgFuncRegionRmData);

    regFunc("dump_region", dbgFuncDumpRegion);
    regFunc("dump_all_region", dbgFuncDumpAllRegion);
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
    if (ori_name.size() > prefix_not_print_res.size() && std::memcmp(ori_name.data(), prefix_not_print_res.data(), prefix_not_print_res.size()) == 0)
    {
        print_res = false;
        name = ori_name.substr(prefix_not_print_res.size() , ori_name.size() - prefix_not_print_res.size());
    }

    auto it = funcs.find(name);
    if (it == funcs.end())
        throw Exception("DBG function not found", ErrorCodes::BAD_ARGUMENTS);

    std::stringstream col_name;
    col_name << name << "(";
    for (size_t i = 0; i < args.size(); ++i)
    {
        std::string arg = args[i]->getColumnName();
        col_name << normalizeArg(arg) << ((i + 1 == args.size()) ? "" : ", ");
    }
    col_name << ")";

    std::shared_ptr<StringStreamBlockInputStream> res = std::make_shared<StringStreamBlockInputStream>(col_name.str());
    Printer printer = [&] (const std::string & s)
    {
        res->append(s);
    };

    (it->second)(context, args, printer);

    return print_res ? res : std::shared_ptr<StringStreamBlockInputStream>();
}

}
