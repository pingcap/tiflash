#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Debug/SerializeExecutor.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <common/StringRef.h>

#include "tipb/schema.pb.h"

namespace DB
{
namespace
{
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
    case TiDB::TypeVarString:
        return "VarString";
    case TiDB::TypeString:
        return "String";
    // case TiDB::TypeBlob:
    //     return "Blob";
    // case TiDB::TypeTinyBlob:

    // case TiDB::TypeMediumBlob:
    // case TiDB::TypeLongBlob:
    // case TiDB::TypeBit:
    // case TiDB::TypeEnum:
    // case TiDB::TypeJSON:
    //     return VAR_SIZE;
    default:
        throw TiFlashException("not supported field type in arrow encode: " + std::to_string(tp), Errors::Coprocessor::Internal);
    }
}
} // namespace


// todo add DAGSchema
void buildTSString(const String & executor_id, const tipb::TableScan & ts, SerializeExecutorContext & serialize_executor_context)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        throw TiFlashException("Table id not specified in table scan executor", Errors::Coprocessor::BadRequest); // todo exception
    }
    // TableID table_id = ts.table_id();
    if (ts.columns_size() == 0)
    {
        // no column selected, must be something wrong
        throw TiFlashException("No column is selected in table scan executor", Errors::Coprocessor::BadRequest);
    }
    // serialize_executor_context.buf.append(executor_id).append("\n");

    serialize_executor_context.buf.fmtAppend("{} columns: {{ ", executor_id);
    serialize_executor_context.buf.joinStr(
        ts.columns().begin(),
        ts.columns().end(),
        [](const auto & ci, FmtBuffer & fb) {
            fb.fmtAppend("column_id: {}, [{}]", ci.column_id(), getFieldTypeName(ci.tp())); // todo figure out how to print column name table name db name..
        },
        ",");
    serialize_executor_context.buf.append("}\n");
}

// todo print more details
void buildExchangeReceiverString(const String & executor_id, const tipb::ExchangeReceiver & exchange_receiver [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void buildSelString(const String & executor_id, const tipb::Selection & sel [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void buildLimitString(const String & executor_id, const tipb::Limit & limit [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void buildProjString(const String & executor_id, const tipb::Projection & proj [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void buildAggString(const String & executor_id, const tipb::Aggregation & agg [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void buildTopNString(const String & executor_id, const tipb::TopN & top_n [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void buildJoinString(const String & executor_id, const tipb::Join & join [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

void buildExchangeSenderString(const String & executor_id, const tipb::ExchangeSender & exchange_sender [[maybe_unused]], SerializeExecutorContext & serialize_executor_context)
{
    serialize_executor_context.buf.append(executor_id).append("\n");
}

} // namespace DB