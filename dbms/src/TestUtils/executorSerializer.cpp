#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <TestUtils/executorSerializer.h>
#include <common/StringRef.h>

namespace DB::tests
{

String ExecutorSerializer::serialize(const tipb::DAGRequest * dag_request)
{
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->has_root_executor())
    {
        serialize(dag_request->root_executor(), 0);
        return context.buf.toString();
    }
    else
    {
        FmtBuffer buffer;
        String prefix;
        traverseExecutors(dag_request, [this, &prefix](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            context.buf.fmtAppend("{}{}\n", prefix, executor.executor_id());
            prefix.append(" ");
            return true;
        });
        return buffer.toString();
    }
}

String getFieldTypeName(Int32 tp)
{
    switch (tp)
    {
    case TiDB::TypeTiny:
        return "tiny";
    case TiDB::TypeShort:
        return "short";
    case TiDB::TypeInt24:
        return "Int24";
    case TiDB::TypeLong:
        return "Long";
    case TiDB::TypeLongLong:
        return "Long long";
    case TiDB::TypeYear:
        return "Year";
    case TiDB::TypeDouble:
        return "Double";
    case TiDB::TypeTime:
        return "Time";
    case TiDB::TypeDate:
        return "Data";
    case TiDB::TypeDatetime:
        return "Datatime";
    case TiDB::TypeNewDate:
        return "NewData";
    case TiDB::TypeTimestamp:
        return "Timestamp";
    case TiDB::TypeFloat:
        return "Float";
    case TiDB::TypeDecimal:
        return "Decimal";
    case TiDB::TypeNewDecimal:
        return "NewDecimal";
    case TiDB::TypeVarchar:
        return "Varchar";
    case TiDB::TypeString:
        return "String";
    default:
        throw TiFlashException("not supported field type in arrow encode: " + std::to_string(tp), Errors::Coprocessor::Internal);
    }
}

void serializeTableScan(const String & executor_id [[maybe_unused]], const tipb::TableScan & ts [[maybe_unused]], ExecutorSerializerContext & serialize_executor_context)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        throw TiFlashException("Table id not specified in table scan executor", Errors::Coprocessor::BadRequest);
    }
    // TableID table_id = ts.table_id();
    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw TiFlashException("No column is selected in table scan executor", Errors::Coprocessor::BadRequest);
    }
    serialize_executor_context.buf.fmtAppend("{} columns: {{ ", executor_id);
    serialize_executor_context.buf.joinStr(
        ts.columns().begin(),
        ts.columns().end(),
        [](const auto & ci, FmtBuffer & fb) {
            fb.fmtAppend("type: {}", ci.column_id(), getFieldTypeName(ci.tp()));
        },
        ", ");
    serialize_executor_context.buf.append("}\n");
}


void serializeSelection(const String & executor_id, const tipb::Selection & sel [[maybe_unused]], ExecutorSerializerContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void serializeLimit(const String & executor_id, const tipb::Limit & limit [[maybe_unused]], ExecutorSerializerContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void serializeProjection(const String & executor_id, const tipb::Projection & proj [[maybe_unused]], ExecutorSerializerContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void serializeAggregation(const String & executor_id, const tipb::Aggregation & agg [[maybe_unused]], ExecutorSerializerContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void serializeTopN(const String & executor_id, const tipb::TopN & top_n [[maybe_unused]], ExecutorSerializerContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void serializeJoin(const String & executor_id, const tipb::Join & join [[maybe_unused]], ExecutorSerializerContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

} // namespace DB::tests