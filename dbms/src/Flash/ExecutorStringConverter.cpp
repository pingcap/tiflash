#include <Common/TiFlashException.h>
#include <Common/joinToString.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/ExecutorStringConverter.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

inline FmtBuffer & appendNamesAndTypes(FmtBuffer & buf, const NamesAndTypes & names_and_types)
{
    joinIter(names_and_types.cbegin(), names_and_types.cend(), buf, [](const auto & nt, FmtBuffer & fb) { fb.template append(nt.name).template append("[").template append(nt.type->getName()).template append("]"); });
    return buf;
}

inline FmtBuffer & appendExprs(FmtBuffer & buf, const google::protobuf::RepeatedPtrField<::tipb::Expr> & exprs, const NamesAndTypes & input_column)
{
    joinIter(exprs.cbegin(), exprs.cend(), buf, [&](const auto & expr, FmtBuffer & fb) { fb.template append(exprToString(expr, input_column)); });
    return buf;
}

inline FmtBuffer & appendByItems(FmtBuffer & buf, const google::protobuf::RepeatedPtrField<::tipb::ByItem> & byItems, const NamesAndTypes & input_column)
{
    joinIter(byItems.cbegin(), byItems.cend(), buf, [&](const auto & byItem, FmtBuffer & fb) { fb.template append(exprToString(byItem.expr(), input_column)); });
    return buf;
}

namespace
{
const std::unordered_map<tipb::ExchangeType, String> exchange_type_map{
    {tipb::PassThrough, "PassThrough"},
    {tipb::Broadcast, "Broadcast"},
    {tipb::Hash, "Hash"}};
} // namespace

inline const String & getExchangeTypeString(tipb::ExchangeType exchange_type)
{
    auto exchange_type_it = exchange_type_map.find(exchange_type);
    if (exchange_type_it == exchange_type_map.end())
        throw TiFlashException("Unknown exchange type", Errors::Coprocessor::Internal);
    return exchange_type_it->second;
}

NamesAndTypes ExecutorStringConverter::buildTSString(const String & executor_id, const tipb::TableScan & ts, FmtBuffer & buf)
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

    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw TiFlashException("No column is selected in table scan executor", Errors::Coprocessor::BadRequest);
    }
    NamesAndTypes columns_from_ts;
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
        auto name = storage->getTableInfo().getColumnName(cid);
        auto pair = storage->getColumns().getPhysical(name);
        columns_from_ts.push_back(pair);
    }
    buf.append(executor_id).append(" (");
    buf.append(storage->getDatabaseName()).append(".").append(storage->getTableName());
    buf.append(" columns: {");
    appendNamesAndTypes(buf, columns_from_ts).append("})");
    return columns_from_ts;
}

NamesAndTypes ExecutorStringConverter::buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, FmtBuffer & buf)
{
    NamesAndTypes columns_from_exchange_receiver;
    for (int i = 0; i < exchange_receiver.field_types_size(); ++i)
    {
        String name = executor_id + "_" + std::to_string(i);
        auto type = getDataTypeByFieldType(exchange_receiver.field_types(i));
        columns_from_exchange_receiver.emplace_back(name, type);
    }
    buf.append(executor_id).append(" (columns: {");
    appendNamesAndTypes(buf, columns_from_exchange_receiver).append("} exchange_type: ");
    buf.append(getExchangeTypeString(exchange_receiver.tp())).append(")");
    return columns_from_exchange_receiver;
}

NamesAndTypes ExecutorStringConverter::buildSelString(const String & executor_id, const tipb::Selection & sel, NamesAndTypes & input_column, FmtBuffer & buf)
{
    buf.append(executor_id).append(" (conditions: {");
    appendExprs(buf, sel.conditions(), input_column).append("})");
    return input_column;
}

NamesAndTypes ExecutorStringConverter::buildLimitString(const String & executor_id, const tipb::Limit & limit, NamesAndTypes & input_column, FmtBuffer & buf)
{
    buf.append(executor_id).append(" (limit: ").append(std::to_string(limit.limit())).append(")");
    return input_column;
}

NamesAndTypes ExecutorStringConverter::buildProjString(const String & executor_id, const tipb::Projection & proj, NamesAndTypes & input_column, FmtBuffer & buf)
{
    NamesAndTypes columns_from_proj;
    for (const auto & expr : proj.exprs())
    {
        auto name = exprToString(expr, input_column);
        auto type = getDataTypeByFieldType(expr.field_type());
        columns_from_proj.emplace_back(name, type);
    }
    buf.append(executor_id).append(" (exprs: {");
    appendNamesAndTypes(buf, columns_from_proj).append("})");
    return columns_from_proj;
}

NamesAndTypes ExecutorStringConverter::buildAggString(const String & executor_id, const tipb::Aggregation & agg, NamesAndTypes & input_column, FmtBuffer & buf)
{
    NamesAndTypes columns_from_agg;
    for (const auto & agg_func : agg.agg_func())
    {
        if (!agg_func.has_field_type())
            throw TiFlashException("Agg func without field type", Errors::Coprocessor::BadRequest);
        auto name = exprToString(agg_func, input_column);
        auto type = getDataTypeByFieldType(agg_func.field_type());
        columns_from_agg.emplace_back(name, type);
    }
    buf.append(executor_id).append(" (agg_funcs: {");
    appendNamesAndTypes(buf, columns_from_agg).append("} group_by: {");
    if (agg.group_by_size() != 0)
    {
        for (const auto & group_by : agg.group_by())
        {
            if (!group_by.has_field_type())
                throw TiFlashException("group by expr without field type", Errors::Coprocessor::BadRequest);
            auto name = exprToString(group_by, input_column);
            auto type = getDataTypeByFieldType(group_by.field_type());
            columns_from_agg.emplace_back(name, type);
        }
        appendExprs(buf, agg.group_by(), input_column);
    }
    buf.append("})");
    return columns_from_agg;
}

NamesAndTypes ExecutorStringConverter::buildTopNString(const String & executor_id, const tipb::TopN & top_n, NamesAndTypes & input_column, FmtBuffer & buf)
{
    buf.append(executor_id).append(" (limit: ").append(std::to_string(top_n.limit())).append(" order_by: {");
    appendByItems(buf, top_n.order_by(), input_column).append("})");
    return input_column;
}

NamesAndTypes ExecutorStringConverter::buildJoinString(const String & executor_id, const tipb::Join & join, NamesAndTypes & left_input_column, NamesAndTypes & right_input_column, FmtBuffer & buf)
{
    buf.append(executor_id).append(" (left_join_keys: {");
    appendExprs(buf, join.left_join_keys(), left_input_column).append("} left_conditions: {");
    appendExprs(buf, join.left_conditions(), left_input_column).append("} right_join_keys: {");
    appendExprs(buf, join.right_join_keys(), right_input_column).append("} right_conditions: {");
    appendExprs(buf, join.right_conditions(), right_input_column).append("} other_conditions: {");
    left_input_column.insert(left_input_column.end(), right_input_column.cbegin(), right_input_column.cend());
    appendExprs(buf, join.other_conditions(), left_input_column).append("})");
    return left_input_column;
}

std::vector<NameAndTypePair> ExecutorStringConverter::buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, NamesAndTypes & input_column, FmtBuffer & buf)
{
    buf.append(executor_id).append(" (columns: {");
    appendNamesAndTypes(buf, input_column).append("} partition_keys: {");
    appendExprs(buf, exchange_sender.partition_keys(), input_column).append("} exchange_type: ");
    buf.append(getExchangeTypeString(exchange_sender.tp())).append(")");
    return input_column;
}

ExecutorStringConverter::ExecutorStringConverter(Context & context_)
    : context(context_)
{}

} // namespace DB