#include <Common/TiFlashException.h>
#include <Common/joinToString.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/MPPStringConverter.h>
#include <IO/Operators.h>
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

template <>
inline WriteBuffer & operator<<(WriteBuffer & buf, const NamesAndTypes & x)
{
    joinIter(x.cbegin(), x.cend(), buf, [](const auto & nt, WriteBuffer & wb) { wb << nt.name << '[' << nt.type->getName() << ']'; });
    return buf;
}

inline WriteBuffer & exprsToString(const google::protobuf::RepeatedPtrField<::tipb::Expr> & exprs, const NamesAndTypes & input_column, WriteBuffer & buf)
{
    joinIter(exprs.cbegin(), exprs.cend(), buf, [&](const auto & expr, WriteBuffer & wb) { wb << exprToString(expr, input_column); });
    return buf;
}

inline WriteBuffer & byItemsToString(const google::protobuf::RepeatedPtrField<::tipb::ByItem> & byItems, const NamesAndTypes & input_column, WriteBuffer & buf)
{
    joinIter(byItems.cbegin(), byItems.cend(), buf, [&](const auto & byItem, WriteBuffer & wb) { wb << exprToString(byItem.expr(), input_column); });
    return buf;
}

namespace
{
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

NamesAndTypes MPPStringConverter::buildTSString(const String & executor_id, const tipb::TableScan & ts, WriteBufferFromOwnString & buf)
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
    buf << genPrefixString() << executor_id << " (";
    buf << storage->getDatabaseName() << "." << storage->getTableName();
    buf << " columns: {" << columns_from_ts << "})";
    return columns_from_ts;
}

NamesAndTypes MPPStringConverter::buildSelString(const String & executor_id, const tipb::Selection & sel, WriteBufferFromOwnString & buf)
{
    auto input_column = buildString(sel.child(), buf);
    String child_str = buf.str();
    buf.restart();
    buf << genPrefixString() << executor_id << " (conditions: {";
    exprsToString(sel.conditions(), input_column, buf) << "})\n";
    buf << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildLimitString(const String & executor_id, const tipb::Limit & limit, WriteBufferFromOwnString & buf)
{
    auto input_column = buildString(limit.child(), buf);
    String child_str = buf.str();
    buf.restart();
    auto limit_count = limit.limit();
    buf << genPrefixString() << executor_id << " (limit: " << limit_count << ")\n";
    buf << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildProjString(const String & executor_id, const tipb::Projection & proj, WriteBufferFromOwnString & buf)
{
    auto input_column = buildString(proj.child(), buf);
    String child_str = buf.str();
    buf.restart();
    NamesAndTypes columns_from_proj;
    for (const auto & expr : proj.exprs())
    {
        auto name = exprToString(expr, input_column);
        auto type = getDataTypeByFieldType(expr.field_type());
        columns_from_proj.emplace_back(name, type);
    }
    buf << genPrefixString() << executor_id << " (exprs: {" << columns_from_proj << "})\n";
    buf << child_str;
    return columns_from_proj;
}

NamesAndTypes MPPStringConverter::buildAggString(const String & executor_id, const tipb::Aggregation & agg, WriteBufferFromOwnString & buf)
{
    auto input_column = buildString(agg.child(), buf);
    String child_str = buf.str();
    buf.restart();
    NamesAndTypes columns_from_agg;
    for (const auto & agg_func : agg.agg_func())
    {
        if (!agg_func.has_field_type())
            throw TiFlashException("Agg func without field type", Errors::Coprocessor::BadRequest);
        auto name = exprToString(agg_func, input_column);
        auto type = getDataTypeByFieldType(agg_func.field_type());
        columns_from_agg.emplace_back(name, type);
    }
    buf << genPrefixString() << executor_id << " (agg_funcs: {" << columns_from_agg << "} group_by: {";
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
        exprsToString(agg.group_by(), input_column, buf);
    }
    buf << "})\n";
    buf << child_str;
    return columns_from_agg;
}

NamesAndTypes MPPStringConverter::buildTopNString(const String & executor_id, const tipb::TopN & topN, WriteBufferFromOwnString & buf)
{
    auto input_column = buildString(topN.child(), buf);
    String child_str = buf.str();
    buf.restart();
    buf << genPrefixString() << executor_id << " (order_by: {";
    byItemsToString(topN.order_by(), input_column, buf) << "})\n";
    buf << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildJoinString(const String & executor_id, const tipb::Join & join, WriteBufferFromOwnString & buf)
{
    if (join.children_size() != 2)
        throw TiFlashException("Join executor children size not equal to 2", Errors::Coprocessor::BadRequest);
    auto left_input_column = buildString(join.children(0), buf);
    String left_child_str = buf.str();
    buf.restart();
    auto right_input_column = buildString(join.children(1), buf);
    String right_child_str = buf.str();
    buf.restart();

    buf << genPrefixString() << executor_id << " (left_join_keys: {";
    exprsToString(join.left_join_keys(), left_input_column, buf) << "} left_conditions: {";
    exprsToString(join.left_conditions(), left_input_column, buf) << "} right_join_keys: {";
    exprsToString(join.right_join_keys(), right_input_column, buf) << "} right_conditions: {";
    exprsToString(join.right_conditions(), right_input_column, buf) << "} other_conditions: {";
    left_input_column.insert(left_input_column.end(), right_input_column.cbegin(), right_input_column.cend());
    exprsToString(join.other_conditions(), left_input_column, buf) << "})\n";
    buf << left_child_str << '\n';
    buf << right_child_str;
    return left_input_column;
}

std::vector<NameAndTypePair> MPPStringConverter::buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender, WriteBufferFromOwnString & buf)
{
    auto input_column = buildString(exchange_sender.child(), buf);
    String child_str = buf.str();
    buf.restart();
    buf << genPrefixString() << executor_id << " (columns: {" << input_column << "} partition_keys: {";
    exprsToString(exchange_sender.partition_keys(), input_column, buf) << "} exchange_type: ";
    buf << getExchangeTypeString(exchange_sender.tp()) << ")\n";
    buf << child_str;
    return input_column;
}

NamesAndTypes MPPStringConverter::buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver, WriteBufferFromOwnString & buf)
{
    NamesAndTypes columns_from_exchange_receiver;
    for (int i = 0; i < exchange_receiver.field_types_size(); ++i)
    {
        String name = executor_id + "_" + std::to_string(i);
        auto type = getDataTypeByFieldType(exchange_receiver.field_types(i));
        columns_from_exchange_receiver.emplace_back(name, type);
    }
    buf << genPrefixString() << executor_id << " (columns: {" << columns_from_exchange_receiver << "} exchange_type: ";
    buf << getExchangeTypeString(exchange_receiver.tp()) << ")";
    return columns_from_exchange_receiver;
}

NamesAndTypes MPPStringConverter::buildString(const tipb::Executor & executor, WriteBufferFromOwnString & buf)
{
    if (!executor.has_executor_id())
        throw TiFlashException("Tree struct based executor must have executor id", Errors::Coprocessor::BadRequest);

    auto counter = CurrentLevelCounter(current_level);
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        return buildTSString(executor.executor_id(), executor.tbl_scan(), buf);
    case tipb::ExecType::TypeJoin:
        return buildJoinString(executor.executor_id(), executor.join(), buf);
    case tipb::ExecType::TypeIndexScan:
        // index scan not supported
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        return buildSelString(executor.executor_id(), executor.selection(), buf);
    case tipb::ExecType::TypeAggregation:
    // stream agg is not supported, treated as normal agg
    case tipb::ExecType::TypeStreamAgg:
        return buildAggString(executor.executor_id(), executor.aggregation(), buf);
    case tipb::ExecType::TypeTopN:
        return buildTopNString(executor.executor_id(), executor.topn(), buf);
    case tipb::ExecType::TypeLimit:
        return buildLimitString(executor.executor_id(), executor.limit(), buf);
    case tipb::ExecType::TypeProjection:
        return buildProjString(executor.executor_id(), executor.projection(), buf);
    case tipb::ExecType::TypeExchangeSender:
        return buildExchangeSenderString(executor.executor_id(), executor.exchange_sender(), buf);
    case tipb::ExecType::TypeExchangeReceiver:
        return buildExchangeReceiverString(executor.executor_id(), executor.exchange_receiver(), buf);
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
    if (!dag_request.has_root_executor())
    {
        throw TiFlashException("dag_request is illegal for mpp query", Errors::Coprocessor::BadRequest);
    }
    const tipb::Executor & executor = dag_request.root_executor();
    WriteBufferFromOwnString mpp_buf;
    buildString(executor, mpp_buf);
    return mpp_buf.releaseStr();
}

} // namespace DB