#include <Common/typeid_cast.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/TMTContext.h>
#include <tipb/select.pb.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

tipb::DAGRequest compileDAG(Context & context, const String & query);
tipb::SelectResponse executeDAG(Context & context, const tipb::DAGRequest & dag_request);

BlockInputStreamPtr dbgFuncDAG(Context & context, const ASTs & args)
{
    if (args.size() < 2 || args.size() > 3)
        throw Exception("Args not matched, should be: query, region-id[, start-ts]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    //    RegionID region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    //    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    //    if (args.size() == 3)
    //        start_ts = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    //    if (start_ts == 0)
    //        start_ts = context.getTMTContext().getPDClient()->getTS();
    //
    //    tipb::DAGRequest dag_request = compileDAG(context, query);
    //    tipb::SelectResponse dag_response = executeDAG(context, dag_request);

    return executeQuery(query, context, true).in;
}

} // namespace DB
