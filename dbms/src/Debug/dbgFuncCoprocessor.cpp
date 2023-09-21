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

#include <Debug/dbgFuncCoprocessor.h>
#include <Debug/dbgFuncCoprocessorUtils.h>
#include <Debug/dbgNaturalDag.h>
#include <Debug/dbgQueryExecutor.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/TMTContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

BlockInputStreamPtr dbgFuncTiDBQuery(Context & context, const ASTs & args)
{
    if (args.empty() || args.size() > 3)
        throw Exception("Args not matched, should be: query[, region-id, dag_prop_string]", ErrorCodes::BAD_ARGUMENTS);

    auto query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = InvalidRegionID;
    if (args.size() >= 2)
        region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);

    String prop_string;
    if (args.size() == 3)
        prop_string = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    DAGProperties properties = getDAGProperties(prop_string);
    properties.start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [query_tasks, func_wrap_output_stream] = compileQuery(
        context,
        query,
        [&](const String & database_name, const String & table_name) {
            auto mapped_database_name = mappedDatabase(context, database_name);
            auto mapped_table_name = mappedTable(context, database_name, table_name);
            auto storage = context.getTable(mapped_database_name, mapped_table_name.second);
            auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
            if (!managed_storage //
                || !(
                    managed_storage->engineType() == ::TiDB::StorageEngine::DT
                    || managed_storage->engineType() == ::TiDB::StorageEngine::TMT))
                throw Exception(
                    database_name + "." + table_name + " is not ManageableStorage",
                    ErrorCodes::BAD_ARGUMENTS);
            return managed_storage->getTableInfo();
        },
        properties);
    return executeQuery(context, region_id, properties, query_tasks, func_wrap_output_stream);
}

BlockInputStreamPtr dbgFuncMockTiDBQuery(Context & context, const ASTs & args)
{
    if (args.size() < 2 || args.size() > 4)
        throw Exception(
            "Args not matched, should be: query, region-id[, start-ts, dag_prop_string]",
            ErrorCodes::BAD_ARGUMENTS);

    auto query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    auto region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    if (args.size() >= 3)
        start_ts = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    if (start_ts == 0)
        start_ts = context.getTMTContext().getPDClient()->getTS();

    String prop_string;
    if (args.size() == 4)
        prop_string = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    DAGProperties properties = getDAGProperties(prop_string);
    properties.start_ts = start_ts;

    auto [query_tasks, func_wrap_output_stream] = compileQuery(
        context,
        query,
        [&](const String & database_name, const String & table_name) {
            return MockTiDB::instance().getTableByName(database_name, table_name)->table_info;
        },
        properties);

    return executeQuery(context, region_id, properties, query_tasks, func_wrap_output_stream);
}


void dbgFuncTiDBQueryFromNaturalDag(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: json_dag_path", ErrorCodes::BAD_ARGUMENTS);

    auto json_dag_path = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    auto dag = NaturalDag(json_dag_path, &Poco::Logger::get("MockDAG"));
    dag.init();
    dag.build(context);
    std::vector<std::pair<int32_t, String>> failed_req_msg_vec;
    int req_idx = 0;
    for (const auto & it : dag.getReqAndRspVec())
    {
        auto && req = it.first;
        auto && res = it.second;
        int32_t req_id = dag.getReqIDVec()[req_idx];
        bool unequal_flag = false;
        bool failed_flag = false;
        String unequal_msg;
        static auto log = Logger::get();
        try
        {
            unequal_flag = runAndCompareDagReq(req, res, context, unequal_msg);
        }
        catch (const Exception & e)
        {
            failed_flag = true;
            unequal_msg = e.message();
        }
        catch (...)
        {
            failed_flag = true;
            unequal_msg = "Unknown execution exception!";
        }

        if (unequal_flag || failed_flag)
        {
            failed_req_msg_vec.push_back(std::make_pair(req_id, unequal_msg));
            if (!dag.continueWhenError())
                break;
        }
        ++req_idx;
    }
    dag.clean(context);
    if (!failed_req_msg_vec.empty())
    {
        output("Invalid");
        FmtBuffer fmt_buf;
        fmt_buf.joinStr(
            failed_req_msg_vec.begin(),
            failed_req_msg_vec.end(),
            [](const auto & pair, FmtBuffer & fb) {
                fb.fmtAppend("request {} failed, msg: {}", pair.first, pair.second);
            },
            "\n");
        throw Exception(fmt_buf.toString(), ErrorCodes::LOGICAL_ERROR);
    }
}
} // namespace DB
