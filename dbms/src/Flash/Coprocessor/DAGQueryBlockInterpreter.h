#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/IInterpreter.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

class Context;
class Region;
using RegionPtr = std::shared_ptr<Region>;

struct RegionInfo
{
    RegionInfo(RegionID region_id_, UInt64 region_version_, UInt64 region_conf_version_,
        const std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & key_ranges_)
        : region_id(region_id_), region_version(region_version_), region_conf_version(region_conf_version_), key_ranges(key_ranges_)
    {}
    const RegionID region_id;
    const UInt64 region_version;
    const UInt64 region_conf_version;
    const std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & key_ranges;
};

struct Pipeline
{
    BlockInputStreams streams;

    BlockInputStreamPtr & firstStream() { return streams.at(0); }

    template <typename Transform>
    void transform(Transform && transform)
    {
        for (auto & stream : streams)
            transform(stream);
    }

    bool hasMoreThanOneStream() const { return streams.size() > 1; }
};

struct AnalysisResult
{
    bool has_where = false;
    bool need_aggregate = false;
    bool has_order_by = false;

    ExpressionActionsPtr before_where;
    ExpressionActionsPtr before_aggregation;
    ExpressionActionsPtr before_order_and_select;
    ExpressionActionsPtr final_projection;

    String filter_column_name;
    Strings order_column_names;
    /// Columns from the SELECT list, before renaming them to aliases.
    Names selected_columns;

    Names aggregation_keys;
    AggregateDescriptions aggregate_descriptions;
};
/** build ch plan from dag request: dag executors -> ch plan
  */
class DAGQueryBlockInterpreter
{
public:
    DAGQueryBlockInterpreter(Context & context_, const std::vector<BlockInputStreams> & input_streams_vec_,
        const DAGQueryBlock & query_block_, bool keep_session_timezone_info_, const std::vector<RegionInfo> & region_infos, const tipb::DAGRequest & rqst,
        ASTPtr dummp_query);

    ~DAGQueryBlockInterpreter() = default;

    BlockInputStreams execute();

private:
    void executeRemoteQuery(Pipeline & pipeline);
    void executeImpl(Pipeline & pipeline);
    void executeTS(const tipb::TableScan & ts, Pipeline & pipeline);
    void executeJoin(const tipb::Join & join, Pipeline & pipeline, SubqueryForSet & right_query);
    void prepareJoinKeys(const tipb::Join & join, Pipeline & pipeline, Names & key_names, bool tiflash_left);
    void executeSubqueryInJoin(Pipeline & pipeline, SubqueryForSet & subquery);
    void executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, String & filter_column);
    void executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr);
    void executeOrder(Pipeline & pipeline, Strings & order_column_names);
    void executeUnion(Pipeline & pipeline);
    void executeLimit(Pipeline & pipeline);
    void executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, Names & aggregation_keys,
        AggregateDescriptions & aggregate_descriptions);
    void executeFinalProject(Pipeline & pipeline);
    void getAndLockStorageWithSchemaVersion(TableID table_id, Int64 schema_version);
    SortDescription getSortDescription(Strings & order_column_names);
    AnalysisResult analyzeExpressions();
    //void recordProfileStreams(Pipeline & pipeline, Int32 index);
    bool addTimeZoneCastAfterTS(std::vector<bool> & is_ts_column, Pipeline & pipeline);
    RegionException::RegionReadStatus getRegionReadStatus(const RegionPtr & current_region);

private:
    Context & context;
    std::vector<BlockInputStreams> input_streams_vec;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;
    const std::vector<RegionInfo> & region_infos;
    const tipb::DAGRequest & rqst;
    ASTPtr dummy_query;

    NamesWithAliases final_project;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /// Table from where to read data, if not subquery.
    ManageableStoragePtr storage;
    TableStructureReadLockPtr table_lock;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;


    bool filter_on_handle = false;
    tipb::Expr handle_filter_expr;
    Int32 handle_col_id = -1;
    std::vector<const tipb::Expr *> conditions;

    Poco::Logger * log;
};
} // namespace DB
