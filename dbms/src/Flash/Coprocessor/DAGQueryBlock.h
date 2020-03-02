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
    explicit DAGQueryBlock(const tipb::Executor * root);
    explicit DAGQueryBlock(std::vector<const tipb::Executor *> & executors);
    const tipb::Executor * source = nullptr;
    const tipb::Executor * selection = nullptr;
    const tipb::Executor * aggregation = nullptr;
    const tipb::Executor * limitOrTopN = nullptr;
    // todo use unique_ptr instead
    std::vector<std::shared_ptr<DAGQueryBlock>> children;
    std::vector<tipb::FieldType> output_field_types;
    // kinds of project
    std::vector<Int32> output_offsets;
    bool is_final_query_block;
};

} // namespace DB
