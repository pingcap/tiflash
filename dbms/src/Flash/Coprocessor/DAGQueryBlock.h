#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/IQuerySource.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class Context;
class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

/// DAGQueryBlock is a dag query from single source,
/// which means the query block contains a source node(tablescan or join)
/// and some of the optional node.(selection/aggregation/project/limit/topN)
class DAGQueryBlock
{
public:
    DAGQueryBlock(UInt32 id, const tipb::Executor & root, TiFlashMetricsPtr metrics);
    DAGQueryBlock(UInt32 id, const ::google::protobuf::RepeatedPtrField<tipb::Executor> & executors, TiFlashMetricsPtr metrics);
    /// the xxx_name is added for compatibility issues: before join is supported, executor does not
    /// has executor name, after join is supported in dag request, every executor has an unique
    /// name(executor->executor_id()). Since We can not always get the executor name from executor
    /// itself, we had to add xxx_name here
    const tipb::Executor * source = nullptr;
    String source_name;
    const tipb::Executor * selection = nullptr;
    String selection_name;
    const tipb::Executor * aggregation = nullptr;
    String aggregation_name;
    const tipb::Executor * having = nullptr;
    String having_name;
    const tipb::Executor * limitOrTopN = nullptr;
    String limitOrTopN_name;
    const tipb::Executor * exchangeSender = nullptr;
    String exchangeServer_name;
    UInt32 id;
    const tipb::Executor * root;
    String qb_column_prefix;
    String qb_join_subquery_alias;
    std::vector<std::shared_ptr<DAGQueryBlock>> children;
    std::vector<tipb::FieldType> output_field_types;
    // kinds of project
    std::vector<Int32> output_offsets;
    void fillOutputFieldTypes();
    void collectAllPossibleChildrenJoinSubqueryAlias(std::unordered_map<UInt32, std::vector<String>> & result);
    bool isRootQueryBlock() const { return id == 1; };
    bool isRemoteQuery() const
    {
        return source->tp() == tipb::ExecType::TypeTableScan && source->tbl_scan().next_read_engine() != tipb::EngineType::Local;
    }
};

} // namespace DB
