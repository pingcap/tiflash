#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Common/joinStr.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/ExecutorToStringUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Storages/Transaction/Types.h>
#include <common/StringRef.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_TABLE;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace
{
inline void appendNamesAndTypes(FmtBuffer & buf, const NamesAndTypes & names_and_types)
{
    joinStr(names_and_types.cbegin(), names_and_types.cend(), buf, [](const auto & nt, FmtBuffer & fb) { fb.append(nt.name).append("[").append(nt.type->getName()).append("]"); });
}

inline void appendExprs(FmtBuffer & buf, const google::protobuf::RepeatedPtrField<::tipb::Expr> & exprs, const NamesAndTypes & input_column)
{
    joinStr(exprs.cbegin(), exprs.cend(), buf, [&](const auto & expr, FmtBuffer & fb) { fb.append(exprToString(expr, input_column)); });
}

inline void appendByItems(FmtBuffer & buf, const google::protobuf::RepeatedPtrField<::tipb::ByItem> & byItems, const NamesAndTypes & input_column)
{
    joinStr(byItems.cbegin(), byItems.cend(), buf, [&](const auto & byItem, FmtBuffer & fb) { fb.append(exprToString(byItem.expr(), input_column)); });
}

const std::unordered_map<tipb::ExchangeType, String> exchange_type_map{
    {tipb::PassThrough, "PassThrough"},
    {tipb::Broadcast, "Broadcast"},
    {tipb::Hash, "Hash"}};

inline StringRef getExchangeTypeString(tipb::ExchangeType exchange_type)
{
    auto exchange_type_it = exchange_type_map.find(exchange_type);
    if (exchange_type_it == exchange_type_map.end())
        throw TiFlashException("Unknown exchange type", Errors::Coprocessor::Internal);
    return exchange_type_it->second;
}

const std::unordered_map<tipb::JoinType, String> join_type_map{
    {tipb::TypeInnerJoin, "Inner"},
    {tipb::TypeLeftOuterJoin, "Left"},
    {tipb::TypeRightOuterJoin, "Right"},
    {tipb::TypeSemiJoin, "Semi"},
    {tipb::TypeAntiSemiJoin, "AntiSemi"},
    {tipb::TypeLeftOuterSemiJoin, "LeftOuterSemi"},
    {tipb::TypeAntiLeftOuterSemiJoin, "AntiLeftOuterSemi"}};

inline StringRef getJoinTypeString(tipb::JoinType join_type)
{
    auto join_type_it = join_type_map.find(join_type);
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type", Errors::Coprocessor::Internal);
    return join_type_it->second;
}
} // namespace

void buildTSString(const String & executor_id, const tipb::TableScan & ts, BuildContext & build_context)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        throw TiFlashException("Table id not specified in table scan executor", Errors::Coprocessor::BadRequest);
    }
    TableID table_id = ts.table_id();
    auto & tmt_ctx = build_context.context.getTMTContext();
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
    NamesAndTypes & columns_from_ts = build_context.newSchema();
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
    FmtBuffer & buf = build_context.buf;
    buf.append(executor_id).append(" (");
    buf.append(storage->getDatabaseName()).append(".").append(storage->getTableName());
    buf.append(" columns: {");
    appendNamesAndTypes(buf, columns_from_ts);
    buf.append("})");
}

void buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, BuildContext & build_context)
{
    NamesAndTypes & columns_from_exchange_receiver = build_context.newSchema();
    for (int i = 0; i != exchange_receiver.field_types_size(); ++i)
    {
        String name = executor_id + "_" + std::to_string(i);
        auto type = getDataTypeByFieldType(exchange_receiver.field_types(i));
        columns_from_exchange_receiver.emplace_back(name, type);
    }
    FmtBuffer & buf = build_context.buf;
    buf.append(executor_id).append(" (columns: {");
    appendNamesAndTypes(buf, columns_from_exchange_receiver);
    buf.append("} exchange_type: ");
    buf.append(getExchangeTypeString(exchange_receiver.tp())).append(")");
}

void buildSelString(const String & executor_id, const tipb::Selection & sel, BuildContext & build_context)
{
    FmtBuffer & buf = build_context.buf;
    buf.append(executor_id).append(" (conditions: {");
    appendExprs(buf, sel.conditions(), build_context.schema());
    buf.append("})");
}

void buildLimitString(const String & executor_id, const tipb::Limit & limit, BuildContext & build_context)
{
    build_context.buf.append(executor_id).fmtAppend(" (limit: {}", limit.limit()).append(")");
}

void buildProjString(const String & executor_id, const tipb::Projection & proj, BuildContext & build_context)
{
    NamesAndTypes input_column = build_context.popBackSchema();
    NamesAndTypes & columns_from_proj = build_context.newSchema();
    for (const auto & expr : proj.exprs())
    {
        auto name = exprToString(expr, input_column);
        auto type = getDataTypeByFieldType(expr.field_type());
        columns_from_proj.emplace_back(name, type);
    }
    FmtBuffer & buf = build_context.buf;
    buf.append(executor_id).append(" (exprs: {");
    appendNamesAndTypes(buf, columns_from_proj);
    buf.append("})");
}

void buildAggString(const String & executor_id, const tipb::Aggregation & agg, BuildContext & build_context)
{
    NamesAndTypes input_column = build_context.popBackSchema();
    NamesAndTypes & columns_from_agg = build_context.newSchema();
    for (const auto & agg_func : agg.agg_func())
    {
        if (!agg_func.has_field_type())
            throw TiFlashException("Agg func without field type", Errors::Coprocessor::BadRequest);
        auto name = exprToString(agg_func, input_column);
        auto type = getDataTypeByFieldType(agg_func.field_type());
        columns_from_agg.emplace_back(name, type);
    }
    FmtBuffer & buf = build_context.buf;
    buf.append(executor_id).append(" (agg_funcs: {");
    appendNamesAndTypes(buf, columns_from_agg);
    buf.append("} group_by: {");
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
}

void buildTopNString(const String & executor_id, const tipb::TopN & top_n, BuildContext & build_context)
{
    FmtBuffer & buf = build_context.buf;
    buf.append(executor_id).fmtAppend(" (limit: {}", top_n.limit()).append(" order_by: {");
    appendByItems(buf, top_n.order_by(), build_context.schema());
    buf.append("})");
}

void buildJoinString(const String & executor_id, const tipb::Join & join, BuildContext & build_context)
{
    auto & buf = build_context.buf;
    if (build_context.schemas.size() < 2)
        throw TiFlashException("schemas.size() should >= 2", Errors::Coprocessor::Internal);
    auto right_input_column = build_context.popBackSchema();
    auto & left_input_column = build_context.schema();

    StringRef join_type = join.has_join_type() ? getJoinTypeString(join.join_type()) : "unknown";
    buf.append(executor_id).append(" (join_type: ").append(join_type).append(" left_join_keys: {");
    appendExprs(buf, join.left_join_keys(), left_input_column);
    buf.append("} left_conditions: {");
    appendExprs(buf, join.left_conditions(), left_input_column);
    buf.append("} right_join_keys: {");
    appendExprs(buf, join.right_join_keys(), right_input_column);
    buf.append("} right_conditions: {");
    appendExprs(buf, join.right_conditions(), right_input_column);
    buf.append("} other_conditions: {");

    NamesAndTypes & columns_from_join = left_input_column;
    columns_from_join.insert(columns_from_join.end(), right_input_column.cbegin(), right_input_column.cend());

    appendExprs(buf, join.other_conditions(), columns_from_join);
    buf.append("} other_eq_conditions_from_in: {");
    appendExprs(buf, join.other_eq_conditions_from_in(), columns_from_join);
    buf.append("})");
}

void buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, BuildContext & build_context)
{
    auto & buf = build_context.buf;
    buf.append(executor_id).append(" (columns: {");
    appendNamesAndTypes(buf, build_context.schema());
    buf.append("} partition_keys: {");
    appendExprs(buf, exchange_sender.partition_keys(), build_context.schema());
    buf.append("} exchange_type: ");
    buf.append(getExchangeTypeString(exchange_sender.tp())).append(")");
}

} // namespace DB