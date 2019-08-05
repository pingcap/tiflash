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

bool DAGStringConverter::buildTSString(const tipb::TableScan & ts, std::stringstream & ss)
{
    TableID id;
    if (ts.has_table_id())
    {
        id = ts.table_id();
    }
    else
    {
        // do not have table id
        return false;
    }
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(id);
    if (storage == nullptr)
    {
        tmt_ctx.getSchemaSyncer()->syncSchema(id, context, false);
        storage = tmt_ctx.getStorages().get(id);
    }
    if (storage == nullptr)
    {
        return false;
    }
    const auto * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get());
    if (!merge_tree)
    {
        return false;
    }

    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        return false;
    }
    columns_from_ts = storage->getColumns().getAllPhysical();
    for (const tipb::ColumnInfo & ci : ts.columns())
    {
        ColumnID cid = ci.column_id();
        if (cid <= 0 || cid > (ColumnID)columns_from_ts.size())
        {
            throw Exception("column id out of bound");
        }
        String name = merge_tree->getTableInfo().columns[cid - 1].name;
        output_from_ts.push_back(std::move(name));
    }
    ss << "FROM " << merge_tree->getTableInfo().db_name << "." << merge_tree->getTableInfo().name << " ";
    return true;
}

bool DAGStringConverter::buildSelString(const tipb::Selection & sel, std::stringstream & ss)
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
    return true;
}

bool DAGStringConverter::buildLimitString(const tipb::Limit & limit, std::stringstream & ss)
{
    ss << "LIMIT " << limit.limit() << " ";
    return true;
}

//todo return the error message
bool DAGStringConverter::buildString(const tipb::Executor & executor, std::stringstream & ss)
{
    switch (executor.tp())
    {
        case tipb::ExecType::TypeTableScan:
            return buildTSString(executor.tbl_scan(), ss);
        case tipb::ExecType::TypeIndexScan:
            // index scan not supported
            return false;
        case tipb::ExecType::TypeSelection:
            return buildSelString(executor.selection(), ss);
        case tipb::ExecType::TypeAggregation:
            // stream agg is not supported, treated as normal agg
        case tipb::ExecType::TypeStreamAgg:
            //todo support agg
            return false;
        case tipb::ExecType::TypeTopN:
            // todo support top n
            return false;
        case tipb::ExecType::TypeLimit:
            return buildLimitString(executor.limit(), ss);
    }
}

bool isProject(const tipb::Executor &)
{
    // currently, project is not pushed so always return false
    return false;
}
DAGStringConverter::DAGStringConverter(Context & context_, const tipb::DAGRequest & dag_request_) : context(context_), dag_request(dag_request_)
{
    afterAgg = false;
}

String DAGStringConverter::buildSqlString()
{
    std::stringstream query_buf;
    std::stringstream project;
    for (const tipb::Executor & executor : dag_request.executors())
    {
        if (!buildString(executor, query_buf))
        {
            return "";
        }
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
