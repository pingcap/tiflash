#include <Common/TiFlashException.h>
#include <Core/QueryProcessingStage.h>
#include <Flash/Coprocessor/DAGStringConverter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int COP_BAD_DAG_REQUEST;
extern const int NOT_IMPLEMENTED;
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
        throw TiFlashException("Table id not specified in table scan executor", Errors::Coprocessor::BadRequest);
    }
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(table_id);
    if (storage == nullptr)
    {
        throw TiFlashException("Table " + std::to_string(table_id) + " doesn't exist.", Errors::Coprocessor::BadRequest);
    }

    const auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
    if (!managed_storage)
    {
        throw TiFlashException("Only Manageable table is supported in DAG request", Errors::Coprocessor::BadRequest);
    }

    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw TiFlashException("No column is selected in table scan executor", Errors::Coprocessor::BadRequest);
    }
    for (const tipb::ColumnInfo & ci : ts.columns())
    {
        ColumnID cid = ci.column_id();
        if (cid == -1)
        {
            // Column ID -1 returns the handle column
            auto pk_handle_col = storage->getTableInfo().getPKHandleColumn();
            auto pair = storage->getColumns().getPhysical(
                pk_handle_col.has_value() ? pk_handle_col->get().name : MutableSupport::tidb_pk_column_name);
            columns_from_ts.push_back(pair);
            continue;
        }
        auto name = managed_storage->getTableInfo().getColumnName(cid);
        auto pair = managed_storage->getColumns().getPhysical(name);
        columns_from_ts.push_back(pair);
    }
    ss << "FROM " << storage->getDatabaseName() << "." << storage->getTableName() << " ";
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

void DAGStringConverter::buildProjString(const tipb::Projection & proj, std::stringstream & ss)
{
    ss << "PROJECTION ";
    bool first = true;
    for (auto & expr : proj.exprs())
    {
        if (first)
            first = false;
        else
            ss << ", ";
        auto name = exprToString(expr, getCurrentColumns());
        ss << name;
    }
}

void DAGStringConverter::buildAggString(const tipb::Aggregation & agg, std::stringstream & ss)
{
    for (auto & agg_func : agg.agg_func())
    {
        if (!agg_func.has_field_type())
            throw TiFlashException("Agg func without field type", Errors::Coprocessor::BadRequest);
        columns_from_agg.emplace_back(exprToString(agg_func, getCurrentColumns()), getDataTypeByFieldType(agg_func.field_type()));
    }
    if (agg.group_by_size() != 0)
    {
        ss << "GROUP BY ";
        bool first = true;
        for (auto & group_by : agg.group_by())
        {
            if (first)
                first = false;
            else
                ss << ", ";
            auto name = exprToString(group_by, getCurrentColumns());
            ss << name;
            if (!group_by.has_field_type())
                throw TiFlashException("group by expr without field type", Errors::Coprocessor::BadRequest);
            columns_from_agg.emplace_back(name, getDataTypeByFieldType(group_by.field_type()));
        }
    }
    afterAgg = true;
}
void DAGStringConverter::buildTopNString(const tipb::TopN & topN, std::stringstream & ss)
{
    ss << "ORDER BY ";
    bool first = true;
    for (auto & order_by_item : topN.order_by())
    {
        if (first)
            first = false;
        else
            ss << ", ";
        ss << exprToString(order_by_item.expr(), getCurrentColumns()) << " ";
        ss << (order_by_item.desc() ? "DESC" : "ASC");
    }
    ss << " LIMIT " << topN.limit() << " ";
}

//todo return the error message
void DAGStringConverter::buildString(const tipb::Executor & executor, std::stringstream & ss)
{
    switch (executor.tp())
    {
        case tipb::ExecType::TypeTableScan:
            return buildTSString(executor.tbl_scan(), ss);
        case tipb::ExecType::TypeJoin:
        case tipb::ExecType::TypeIndexScan:
            // index scan not supported
            throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
        case tipb::ExecType::TypeSelection:
            return buildSelString(executor.selection(), ss);
        case tipb::ExecType::TypeAggregation:
            // stream agg is not supported, treated as normal agg
        case tipb::ExecType::TypeStreamAgg:
            return buildAggString(executor.aggregation(), ss);
        case tipb::ExecType::TypeTopN:
            return buildTopNString(executor.topn(), ss);
        case tipb::ExecType::TypeLimit:
            return buildLimitString(executor.limit(), ss);
        case tipb::ExecType::TypeProjection:
            return buildProjString(executor.projection(), ss);
        case tipb::ExecType::TypeExchangeSender:
        case tipb::ExecType::TypeExchangeReceiver:
            throw TiFlashException("Mpp executor is not supported", Errors::Coprocessor::Unimplemented);
        case tipb::ExecType::TypeKill:
            throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
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
        auto current_columns = getCurrentColumns();
        std::vector<UInt64> output_index;
        if (afterAgg)
            for (UInt64 i = 0; i < current_columns.size(); i++)
                output_index.push_back(i);
        else
            for (UInt64 index : dag_request.output_offsets())
                output_index.push_back(index);

        for (UInt64 index : output_index)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                project << ", ";
            }
            project << current_columns[index].name;
        }
        project << " ";
    }
    return project.str() + query_buf.str();
}

} // namespace DB
