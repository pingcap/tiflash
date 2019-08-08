#include <Common/typeid_cast.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <tipb/select.pb.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

using DAGField = std::pair<String, tipb::FieldType>;
using DAGSchema = std::vector<DAGField>;
using SchemaFetcher = std::function<TiDB::TableInfo(const String &, const String &)>;
std::tuple<TableID, DAGSchema, tipb::DAGRequest> compileQuery(
    Context & context, const String & query, SchemaFetcher schema_fetcher, Timestamp start_ts);
tipb::SelectResponse executeDAGRequest(
    Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version, UInt64 region_conf_version);
BlockInputStreamPtr outputDAGResponse(Context & context, const DAGSchema & schema, const tipb::SelectResponse & dag_response);

BlockInputStreamPtr dbgFuncDAG(Context & context, const ASTs & args)
{
    if (args.size() < 1 || args.size() > 2)
        throw Exception("Args not matched, should be: query[, region-id]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = InvalidRegionID;
    if (args.size() == 2)
        region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    Timestamp start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [table_id, schema, dag_request] = compileQuery(context, query,
        [&](const String & database_name, const String & table_name) {
            auto storage = context.getTable(database_name, table_name);
            auto mmt = std::dynamic_pointer_cast<StorageMergeTree>(storage);
            if (!mmt || mmt->getData().merging_params.mode != MergeTreeData::MergingParams::Txn)
                throw Exception("Not TMT", ErrorCodes::BAD_ARGUMENTS);
            return mmt->getTableInfo();
        },
        start_ts);

    RegionPtr region;
    if (region_id == InvalidRegionID)
    {
        auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(table_id);
        if (regions.empty())
            throw Exception("No region for table", ErrorCodes::BAD_ARGUMENTS);
        region = context.getTMTContext().getRegionTable().getRegionsByTable(table_id).front().second;
    }
    else
    {
        region = context.getTMTContext().getRegionTable().getRegionByTableAndID(table_id, region_id);
        if (!region)
            throw Exception("No such region", ErrorCodes::BAD_ARGUMENTS);
    }
    tipb::SelectResponse dag_response = executeDAGRequest(context, dag_request, region_id, region->version(), region->confVer());

    return outputDAGResponse(context, schema, dag_response);
}

BlockInputStreamPtr dbgFuncMockDAG(Context & context, const ASTs & args)
{
    if (args.size() < 2 || args.size() > 3)
        throw Exception("Args not matched, should be: query, region-id[, start-ts]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    if (args.size() == 3)
        start_ts = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    if (start_ts == 0)
        start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [table_id, schema, dag_request] = compileQuery(context, query,
        [&](const String & database_name, const String & table_name) {
            return MockTiDB::instance().getTableByName(database_name, table_name)->table_info;
        },
        start_ts);

    RegionPtr region = context.getTMTContext().getKVStore()->getRegion(region_id);
    tipb::SelectResponse dag_response = executeDAGRequest(context, dag_request, region_id, region->version(), region->confVer());

    return outputDAGResponse(context, schema, dag_response);
}

std::tuple<TableID, DAGSchema, tipb::DAGRequest> compileQuery(
    Context & context, const String & query, SchemaFetcher schema_fetcher, Timestamp start_ts)
{
    DAGSchema schema;
    tipb::DAGRequest dag_request;

    dag_request.set_start_ts(start_ts);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "from DAG compiler", 0);
    ASTSelectQuery & ast_query = typeid_cast<ASTSelectQuery &>(*ast);

    String database_name, table_name;
    auto query_database = ast_query.database();
    auto query_table = ast_query.table();
    if (query_database)
        database_name = typeid_cast<ASTIdentifier &>(*query_database).name;
    if (query_table)
        table_name = typeid_cast<ASTIdentifier &>(*query_table).name;
    if (!query_table)
    {
        database_name = "system";
        table_name = "one";
    }
    else if (!query_database)
    {
        database_name = context.getCurrentDatabase();
    }
    auto table_info = schema_fetcher(database_name, table_name);

    tipb::Executor * executor = dag_request.add_executors();
    executor->set_tp(tipb::ExecType::TypeTableScan);
    tipb::TableScan * ts = executor->mutable_tbl_scan();
    ts->set_table_id(table_info.id);
    size_t i = 0;
    for (const auto & column_info : table_info.columns)
    {
        tipb::ColumnInfo * ci = ts->add_columns();
        ci->set_column_id(column_info.id);
        ci->set_tp(column_info.tp);
        ci->set_flag(column_info.flag);

        tipb::FieldType field_type;
        field_type.set_tp(column_info.tp);
        field_type.set_flag(column_info.flag);
        field_type.set_flen(column_info.flen);
        field_type.set_decimal(column_info.decimal);
        schema.emplace_back(std::make_pair(column_info.name, std::move(field_type)));

        dag_request.add_output_offsets(i);

        i++;
    }

    return std::make_tuple(table_info.id, std::move(schema), std::move(dag_request));
}

tipb::SelectResponse executeDAGRequest(
    Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version, UInt64 region_conf_version)
{
    tipb::SelectResponse dag_response;
    DAGDriver driver(context, dag_request, region_id, region_version, region_conf_version, dag_response, true);
    driver.execute();
    return dag_response;
}

BlockInputStreamPtr outputDAGResponse(Context &, const DAGSchema & schema, const tipb::SelectResponse & dag_response)
{
    BlocksList blocks;
    for (const auto & chunk : dag_response.chunks())
    {
        std::vector<std::vector<DB::Field>> rows;
        std::vector<DB::Field> curr_row;
        const std::string & data = chunk.rows_data();
        size_t cursor = 0;
        while (cursor < data.size())
        {
            curr_row.push_back(DB::DecodeDatum(cursor, data));
            if (curr_row.size() == schema.size())
            {
                rows.emplace_back(std::move(curr_row));
                curr_row.clear();
            }
        }

        ColumnsWithTypeAndName columns;
        for (auto & field : schema)
        {
            const auto & name = field.first;
            auto data_type = getDataTypeByFieldType(field.second);
            ColumnWithTypeAndName col(data_type, name);
            col.column->assumeMutable()->reserve(rows.size());
            columns.emplace_back(std::move(col));
        }
        for (const auto & row : rows)
        {
            for (size_t i = 0; i < row.size(); i++)
            {
                columns[i].column->assumeMutable()->insert(row[i]);
            }
        }

        blocks.emplace_back(Block(columns));
    }

    return std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
}

} // namespace DB
