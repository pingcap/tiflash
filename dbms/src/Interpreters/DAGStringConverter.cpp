#include <Interpreters/DAGStringConverter.h>

#include <Core/QueryProcessingStage.h>
#include <Interpreters/DAGUtils.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

void DAGStringConverter::buildTSString(const tipb::TableScan & ts, std::stringstream & ss)
{
    TableID table_id;
    if (ts.has_table_id())
    {
        table_id = ts.table_id();
    }
    else
    {
        // do not have table id
        throw Exception("Table id not specified in table scan executor", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(table_id);
    if (storage == nullptr)
    {
        throw Exception("Table " + std::to_string(table_id) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
    }
    const auto * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get());
    if (!merge_tree)
    {
        throw Exception("Only MergeTree table is supported in DAG request", ErrorCodes::COP_BAD_DAG_REQUEST);
    }

    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw Exception("No column is selected in table scan executor", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    columns_from_ts = storage->getColumns().getAllPhysical();
    for (const tipb::ColumnInfo & ci : ts.columns())
    {
        ColumnID cid = ci.column_id();
        if (cid <= 0 || cid > (ColumnID)columns_from_ts.size())
        {
            throw Exception("column id out of bound", ErrorCodes::COP_BAD_DAG_REQUEST);
        }
        String name = merge_tree->getTableInfo().columns[cid - 1].name;
        output_from_ts.push_back(std::move(name));
    }
    ss << "FROM " << merge_tree->getTableInfo().db_name << "." << merge_tree->getTableInfo().name << " ";
}

void DAGStringConverter::buildSelString(const tipb::Selection & sel, std::stringstream & ss)
{
    bool first = true;
    for (const tipb::Expr & expr : sel.conditions())
    {
        auto s = exprToString(expr, getCurrentColumns());
        if (first)
        {
            ss << "WHERE ";
            first = false;
        }
        else
        {
            ss << "AND ";
        }
        ss << s << " ";
    }
}

void DAGStringConverter::buildLimitString(const tipb::Limit & limit, std::stringstream & ss) { ss << "LIMIT " << limit.limit() << " "; }

//todo return the error message
void DAGStringConverter::buildString(const tipb::Executor & executor, std::stringstream & ss)
{
    switch (executor.tp())
    {
        case tipb::ExecType::TypeTableScan:
            return buildTSString(executor.tbl_scan(), ss);
        case tipb::ExecType::TypeIndexScan:
            // index scan not supported
            throw Exception("IndexScan is not supported", ErrorCodes::COP_BAD_DAG_REQUEST);
        case tipb::ExecType::TypeSelection:
            return buildSelString(executor.selection(), ss);
        case tipb::ExecType::TypeAggregation:
            // stream agg is not supported, treated as normal agg
        case tipb::ExecType::TypeStreamAgg:
            //todo support agg
            throw Exception("Aggregation is not supported", ErrorCodes::COP_BAD_DAG_REQUEST);
        case tipb::ExecType::TypeTopN:
            // todo support top n
            throw Exception("TopN is not supported", ErrorCodes::COP_BAD_DAG_REQUEST);
        case tipb::ExecType::TypeLimit:
            return buildLimitString(executor.limit(), ss);
    }
}

bool isProject(const tipb::Executor &)
{
    // currently, project is not pushed so always return false
    return false;
}
DAGStringConverter::DAGStringConverter(Context & context_, const tipb::DAGRequest & dag_request_)
    : context(context_), dag_request(dag_request_)
{
    afterAgg = false;
}

String DAGStringConverter::buildSqlString()
{
    std::stringstream query_buf;
    std::stringstream project;
    for (const tipb::Executor & executor : dag_request.executors())
    {
        buildString(executor, query_buf);
    }
    if (!isProject(dag_request.executors(dag_request.executors_size() - 1)))
    {
        //append final project
        project << "SELECT ";
        bool first = true;
        for (UInt32 index : dag_request.output_offsets())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                project << ", ";
            }
            project << getCurrentOutputColumns()[index];
        }
        project << " ";
    }
    return project.str() + query_buf.str();
}

} // namespace DB
