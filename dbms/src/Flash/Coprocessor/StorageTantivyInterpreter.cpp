#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MultiplexInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Coprocessor/StorageTantivyInterpreter.h>
#include <Flash/Coprocessor/collectOutputFieldTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Operators/BlockInputStreamSourceOp.h>
#include <Operators/ConcatSourceOp.h>
#include <Operators/CoprocessorReaderSourceOp.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/NullSourceOp.h>
#include <Operators/UnorderedSourceOp.h>
#include <Parsers/makeDummyQuery.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/LockException.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/MutableSupport.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/S3/S3Common.h>
#include <Storages/StorageDeltaMerge.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/logger_useful.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>


namespace DB
{

void StorageTantivyIterpreter::generateSelectQueryInfos()
{
    auto & dag_context = *context.getDAGContext();
    query_info.query = dag_context.dummy_ast;
    const google::protobuf::RepeatedPtrField<tipb::Expr> filters{};
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{};
    const auto ann_query_info = tipb::ANNQueryInfo{};
    TiDB::ColumnInfos source_columns{};
    const std::vector<int> runtime_filter_ids;
    auto dag_query = std::make_unique<DAGQueryInfo>(
        filters,
        ann_query_info,
        pushed_down_filters,
        tici_scan.getColumns(),
        std::vector<int>{},
        0,
        context.getTimezoneInfo());

    query_info.req_id = fmt::format("{} table_id={}", log->identifier(), 0);
}
} // namespace DB
