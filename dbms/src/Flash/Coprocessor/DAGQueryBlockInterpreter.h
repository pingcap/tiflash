#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/TiDB.h>
#include <pingcap/coprocessor/Client.h>

namespace DB
{

class Context;
class Region;
using RegionPtr = std::shared_ptr<Region>;
struct RegionLearnerReadSnapshot;
using LearnerReadSnapshot = std::unordered_map<RegionID, RegionLearnerReadSnapshot>;
struct SelectQueryInfo;

using DAGColumnInfo = std::pair<String, TiDB::ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;
class DAGQuerySource;
class DAGQueryBlock;
class RegionInfo;
class ExchangeReceiver;
class DAGExpressionAnalyzer;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
struct ExpressionActionsChain;
class IManageableStorage;
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;
using NameWithAlias = std::pair<std::string, std::string>;
using NamesWithAliases = std::vector<NameWithAlias>;

struct Pipeline
{
    BlockInputStreams streams;
    /** When executing FULL or RIGHT JOIN, there will be a data stream from which you can read "not joined" rows.
      * It has a special meaning, since reading from it should be done after reading from the main streams.
      * It is appended to the main streams in UnionBlockInputStream or ParallelAggregatingBlockInputStream.
      */
    BlockInputStreamPtr stream_with_non_joined_data;

    BlockInputStreamPtr & firstStream() { return streams.at(0); }

    template <typename Transform>
    void transform(Transform && transform)
    {
        for (auto & stream : streams)
            transform(stream);
        if (stream_with_non_joined_data)
            transform(stream_with_non_joined_data);
    }

    bool hasMoreThanOneStream() const { return streams.size() + (stream_with_non_joined_data ? 1 : 0) > 1; }
};

struct AnalysisResult
{
    bool need_timezone_cast_after_tablescan = false;
    bool has_where = false;
    bool need_aggregate = false;
    bool has_order_by = false;

    ExpressionActionsPtr timezone_cast;
    ExpressionActionsPtr before_where;
    ExpressionActionsPtr before_aggregation;
    ExpressionActionsPtr before_order_and_select;
    ExpressionActionsPtr final_projection;

    String filter_column_name;
    std::vector<NameAndTypePair> order_columns;
    /// Columns from the SELECT list, before renaming them to aliases.
    Names selected_columns;

    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    AggregateDescriptions aggregate_descriptions;
};
/** build ch plan from dag request: dag executors -> ch plan
  */
class DAGQueryBlockInterpreter
{
public:
    DAGQueryBlockInterpreter(Context & context_, const std::vector<BlockInputStreams> & input_streams_vec_,
        const DAGQueryBlock & query_block_, bool keep_session_timezone_info_, const tipb::DAGRequest & rqst, ASTPtr dummp_query,
        const DAGQuerySource & dag_, std::vector<SubqueriesForSets> & subqueriesForSets_,
        const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map);

    ~DAGQueryBlockInterpreter() = default;

    BlockInputStreams execute();

    static void executeUnion(Pipeline & pipeline, size_t max_streams);

private:
    void executeRemoteQuery(Pipeline & pipeline);
    void executeImpl(Pipeline & pipeline);
    void executeTS(const tipb::TableScan & ts, Pipeline & pipeline);
    void executeJoin(const tipb::Join & join, Pipeline & pipeline, SubqueryForSet & right_query);
    void prepareJoin(const google::protobuf::RepeatedPtrField<tipb::Expr> & keys, const DataTypes & key_types, Pipeline & pipeline,
        Names & key_names, bool left, bool is_right_out_join, const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
        String & filter_column_name);
    ExpressionActionsPtr genJoinOtherConditionAction(const tipb::Join & join, std::vector<NameAndTypePair> & source_columns,
        String & filter_column_for_other_condition, String & filter_column_for_other_eq_condition);
    void executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, String & filter_column);
    void executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr);
    void executeOrder(Pipeline & pipeline, std::vector<NameAndTypePair> & order_columns);
    void executeLimit(Pipeline & pipeline);
    void executeAggregation(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, Names & aggregation_keys,
        TiDB::TiDBCollators & collators, AggregateDescriptions & aggregate_descriptions);
    void executeProject(Pipeline & pipeline, NamesWithAliases & project_cols);

    void readFromLocalStorage(            //
        const TableStructureLockHolder &, //
        const TableID table_id, const Names & required_columns, SelectQueryInfo & query_info, const size_t max_block_size,
        const LearnerReadSnapshot & learner_read_snapshot, //
        Pipeline & pipeline, std::unordered_map<RegionID, const RegionInfo &> & region_retry);
    std::tuple<ManageableStoragePtr, TableStructureLockHolder> getAndLockStorageWithSchemaVersion(TableID table_id, Int64 schema_version);
    SortDescription getSortDescription(std::vector<NameAndTypePair> & order_columns);
    AnalysisResult analyzeExpressions();
    void recordProfileStreams(Pipeline & pipeline, const String & key);
    bool addTimeZoneCastAfterTS(std::vector<bool> & is_ts_column, ExpressionActionsChain & chain);

private:
    void executeRemoteQueryImpl(Pipeline & pipeline, const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
        ::tipb::DAGRequest & dag_req, const DAGSchema & schema);

    Context & context;
    std::vector<BlockInputStreams> input_streams_vec;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;
    const tipb::DAGRequest & rqst;
    ASTPtr dummy_query;

    NamesWithAliases final_project;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /// Table from where to read data, if not subquery.
    ManageableStoragePtr storage;
    TableLockHolder table_drop_lock;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;


    std::vector<const tipb::Expr *> conditions;
    const DAGQuerySource & dag;
    std::vector<SubqueriesForSets> & subqueriesForSets;
    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map;
    std::vector<bool> timestamp_column_flag_for_tablescan;

    Poco::Logger * log;
};
} // namespace DB
