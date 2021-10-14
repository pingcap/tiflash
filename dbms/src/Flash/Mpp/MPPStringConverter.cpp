#include <Common/TiFlashException.h>
#include <Core/QueryProcessingStage.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/MPPStringConverter.h>
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

void namesAndTypesToString(const NamesAndTypes & namesAndTypes, std::stringstream & ss)
{
    if (namesAndTypes.empty())
    {
        return;
    }
    auto iter = namesAndTypes.cbegin();
    ss << iter->name << '[' << iter->type->getName() << ']';
    ++iter;
    for (; iter != namesAndTypes.cend(); ++iter)
    {
        ss << ", " << iter->name << '[' << iter->type->getName() << ']';
    }
}

void exprsToString(const google::protobuf::RepeatedPtrField<::tipb::Expr> & exprs, const NamesAndTypes & input_column, std::stringstream & ss)
{
    if (exprs.empty())
    {
        return;
    }
    auto iter = exprs.cbegin();
    ss << exprToString(*iter, input_column);
    ++iter;
    for (; iter != exprs.cend(); ++iter)
    {
        ss << ", " << exprToString(*iter, input_column);
    }
}

void byItemsToString(const google::protobuf::RepeatedPtrField<::tipb::ByItem> & byItems, const NamesAndTypes & input_column, std::stringstream & ss)
{
    if (byItems.empty())
    {
        return;
    }

    auto iter = byItems.cbegin();
    ss << exprToString(iter->expr(), input_column);
    ++iter;
    for (; iter != byItems.cend(); ++iter)
    {
        ss << ", " << exprToString(iter->expr(), input_column);
    }
}

NamesAndTypes MPPStringConverter::buildTSString(const String & executor_id, const tipb::TableScan & ts, std::stringstream & ss)
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
    ss << genPrefixString() << executor_id << " ( ";
    ss << storage->getDatabaseName() << "." << storage->getTableName();
    ss << " columns: {";
    namesAndTypesToString(columns_from_ts, ss);
    ss << "} )";
    return columns_from_ts;
}

NamesAndTypes MPPStringConverter::buildSelString(const String & executor_id, const tipb::Selection & sel, std::stringstream & ss)
{
    auto input_column = buildString(sel.child(), ss);
    String child_str = ss.str();
    ss.str("");
    ss << genPrefixString() << executor_id << " ( conditions: {";
    exprsToString(sel.conditions(), input_column, ss);
    ss << "} )\n"
       << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildLimitString(const String & executor_id, const tipb::Limit & limit, std::stringstream & ss)
{
    auto input_column = buildString(limit.child(), ss);
    String child_str = ss.str();
    ss.str("");
    auto limit_count = limit.limit();
    ss << genPrefixString() << executor_id << " ( limit_count: " << limit_count << " )\n"
       << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildProjString(const String & executor_id, const tipb::Projection & proj, std::stringstream & ss)
{
    auto input_column = buildString(proj.child(), ss);
    String child_str = ss.str();
    ss.str("");
    NamesAndTypes columns_from_proj;
    for (const auto & expr : proj.exprs())
    {
        auto name = exprToString(expr, input_column);
        auto type = getDataTypeByFieldType(expr.field_type());
        columns_from_proj.emplace_back(name, type);
    }
    ss << genPrefixString() << executor_id << " ( exprs: {";
    namesAndTypesToString(columns_from_proj, ss);
    ss << "} )\n"
       << child_str;
    return columns_from_proj;
}

NamesAndTypes MPPStringConverter::buildAggString(const String & executor_id, const tipb::Aggregation & agg, std::stringstream & ss)
{
    auto input_column = buildString(agg.child(), ss);
    String child_str = ss.str();
    ss.str("");
    NamesAndTypes columns_from_agg;
    for (const auto & agg_func : agg.agg_func())
    {
        if (!agg_func.has_field_type())
            throw TiFlashException("Agg func without field type", Errors::Coprocessor::BadRequest);
        auto name = exprToString(agg_func, input_column);
        auto type = getDataTypeByFieldType(agg_func.field_type());
        columns_from_agg.emplace_back(name, type);
    }
    ss << genPrefixString() << executor_id << " ( agg_funcs: {";
    namesAndTypesToString(columns_from_agg, ss);
    ss << "} group_by: {";
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
        exprsToString(agg.group_by(), input_column, ss);
    }
    ss << "} )\n"
       << child_str;
    return columns_from_agg;
}

NamesAndTypes MPPStringConverter::buildTopNString(const String & executor_id, const tipb::TopN & topN, std::stringstream & ss)
{
    auto input_column = buildString(topN.child(), ss);
    String child_str = ss.str();
    ss.str("");
    ss << genPrefixString() << executor_id << " ( order_by: {";
    byItemsToString(topN.order_by(), input_column, ss);
    ss << "} )\n"
       << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildJoinString(const String & executor_id, const tipb::Join & join, std::stringstream & ss)
{
    if (join.children_size() != 2)
        throw TiFlashException("Join executor children size not equal to 2", Errors::Coprocessor::BadRequest);
    auto left_input_column = buildString(join.children(0), ss);
    String left_child_str = ss.str();
    ss.str("");
    auto right_input_column = buildString(join.children(1), ss);
    String right_child_str = ss.str();
    ss.str("");

    ss << genPrefixString() << executor_id << " ( left_join_keys: {";
    exprsToString(join.left_join_keys(), left_input_column, ss);
    ss << "} left_conditions: {";
    exprsToString(join.left_conditions(), left_input_column, ss);
    ss << "} right_join_keys: {";
    exprsToString(join.right_join_keys(), right_input_column, ss);
    ss << "} right_conditions: {";
    exprsToString(join.right_conditions(), right_input_column, ss);
    ss << "} other_conditions: {";
    left_input_column.insert(left_input_column.end(), right_input_column.cbegin(), right_input_column.cend());
    exprsToString(join.other_conditions(), left_input_column, ss);
    ss << "} )\n"
       << left_child_str << '\n'
       << right_child_str;
    return left_input_column;
}

std::vector<NameAndTypePair> MPPStringConverter::buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, std::stringstream & ss)
{
    auto input_column = buildString(exchange_sender.child(), ss);
    String child_str = ss.str();
    ss.str("");
    ss << genPrefixString() << executor_id << " ( send_columns: {";
    namesAndTypesToString(input_column, ss);
    ss << "} partition_keys: {";
    exprsToString(exchange_sender.partition_keys(), input_column, ss);
    ss << "} )\n"
       << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, std::stringstream & ss)
{
    NamesAndTypes columns_from_exchange_receiver;
    for (int i = 0; i < exchange_receiver.field_types_size(); ++i)
    {
        String name = executor_id + "_" + std::to_string(i);
        auto type = getDataTypeByFieldType(exchange_receiver.field_types(i));
        columns_from_exchange_receiver.emplace_back(name, type);
    }
    ss << genPrefixString() << executor_id << " ( columns: {";
    namesAndTypesToString(columns_from_exchange_receiver, ss);
    ss << "} )";
    return columns_from_exchange_receiver;
}

struct CurrentLevelCounter
{
    size_t & current_level;
    explicit CurrentLevelCounter(size_t & level)
        : current_level(level)
    {
        ++current_level;
    }

    ~CurrentLevelCounter()
    {
        --current_level;
    }
};

NamesAndTypes MPPStringConverter::buildString(const tipb::Executor & executor, std::stringstream & ss)
{
    if (!executor.has_executor_id())
        throw TiFlashException("Tree struct based executor must have executor id", Errors::Coprocessor::BadRequest);

    auto counter = CurrentLevelCounter(current_level);
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        return buildTSString(executor.executor_id(), executor.tbl_scan(), ss);
    case tipb::ExecType::TypeJoin:
        return buildJoinString(executor.executor_id(), executor.join(), ss);
    case tipb::ExecType::TypeIndexScan:
        // index scan not supported
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        return buildSelString(executor.executor_id(), executor.selection(), ss);
    case tipb::ExecType::TypeAggregation:
        // stream agg is not supported, treated as normal agg
    case tipb::ExecType::TypeStreamAgg:
        return buildAggString(executor.executor_id(), executor.aggregation(), ss);
    case tipb::ExecType::TypeTopN:
        return buildTopNString(executor.executor_id(), executor.topn(), ss);
    case tipb::ExecType::TypeLimit:
        return buildLimitString(executor.executor_id(), executor.limit(), ss);
    case tipb::ExecType::TypeProjection:
        return buildProjString(executor.executor_id(), executor.projection(), ss);
    case tipb::ExecType::TypeExchangeSender:
        return buildExchangeSenderString(executor.executor_id(), executor.exchange_sender(), ss);
    case tipb::ExecType::TypeExchangeReceiver:
        return buildExchangeReceiverString(executor.executor_id(), executor.exchange_receiver(), ss);
    case tipb::ExecType::TypeKill:
        throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
    default:
        throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
    }
}

MPPStringConverter::MPPStringConverter(Context & context_, const tipb::DAGRequest & dag_request_)
    : context(context_)
    , dag_request(dag_request_)
{}

String MPPStringConverter::buildMPPString()
{
    std::stringstream mpp_buf;
    if (!dag_request.has_root_executor())
    {
        throw TiFlashException("dag_request is illegal for mpp query", Errors::Coprocessor::BadRequest);
    }
    const tipb::Executor & executor = dag_request.root_executor();
    buildString(executor, mpp_buf);
    return mpp_buf.str();
}

} // namespace DB
