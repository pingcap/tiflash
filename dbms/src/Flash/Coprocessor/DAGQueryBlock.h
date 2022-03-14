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

class QueryBlockIDGenerator
{
    UInt32 current_id = 0; //Root query block id is 1, so set current_id initial value to 0
public:
    UInt32 nextBlockID()
    {
        return ++current_id;
    }
};

/// DAGQueryBlock is a dag query from single source,
/// which means the query block contains a source node(tablescan or join)
/// and some of the optional node.(selection/aggregation/project/limit/topN)
class DAGQueryBlock
{
public:
    DAGQueryBlock(const tipb::Executor & root, QueryBlockIDGenerator & id_generator);
    DAGQueryBlock(UInt32 id, const ::google::protobuf::RepeatedPtrField<tipb::Executor> & executors);
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
    std::map<String, const tipb::Executor *> windows;
    std::map<String, const tipb::Executor *> window_sorts;
    std::vector<String> window_op_list;
    const tipb::Executor * having = nullptr;
    String having_name;
    const tipb::Executor * limit_or_topn = nullptr;
    String limit_or_topn_name;
    const tipb::Executor * exchange_sender = nullptr;
    String exchange_sender_name;
    UInt32 id;
    const tipb::Executor * root;
    String qb_column_prefix;
    std::vector<std::shared_ptr<DAGQueryBlock>> children;

    // only meaningful for root query block.
    std::vector<tipb::FieldType> output_field_types;
    std::vector<Int32> output_offsets;

    bool isRootQueryBlock() const { return id == 1; };
    bool isTableScanSource() const { return source->tp() == tipb::ExecType::TypeTableScan || source->tp() == tipb::ExecType::TypePartitionTableScan; }
};

} // namespace DB
