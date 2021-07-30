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
struct RegionInfo;
class ExchangeReceiver;
class DAGExpressionAnalyzer;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
struct ExpressionActionsChain;
class IManageableStorage;
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;
using NameWithAlias = std::pair<std::string, std::string>;
using NamesWithAliases = std::vector<NameWithAlias>;
using RegionRetryList = std::list<std::reference_wrapper<const RegionInfo>>;

struct DAGPipeline
{
    BlockInputStreams streams;
    /** When executing FULL or RIGHT JOIN, there will be a data stream from which you can read "not joined" rows.
      * It has a special meaning, since reading from it should be done after reading from the main streams.
      * It is appended to the main streams in UnionBlockInputStream or ParallelAggregatingBlockInputStream.
      */
    BlockInputStreams streams_with_non_joined_data;

    BlockInputStreamPtr & firstStream() { return streams.at(0); }

    template <typename Transform>
    void transform(Transform && transform)
    {
        for (auto & stream : streams)
            transform(stream);
        for (auto & stream : streams_with_non_joined_data)
            transform(stream);
    }

    bool hasMoreThanOneStream() const { return streams.size() + streams_with_non_joined_data.size() > 1; }
};

/** build ch plan from dag request: dag executors -> ch plan
  */
class DAGQueryBlockInterpreter
{
public:
    DAGQueryBlockInterpreter(Context & context_, const std::vector<BlockInputStreams> & input_streams_vec_,
        const DAGQueryBlock & query_block_, bool keep_session_timezone_info_, const tipb::DAGRequest & rqst,
        const DAGQuerySource & dag_, std::vector<SubqueriesForSets> & subqueriesForSets_,
        const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map);

    ~DAGQueryBlockInterpreter() = default;

    BlockInputStreams execute();

    static void executeUnion(DAGPipeline & pipeline, size_t max_streams);

private:
    void executeRemoteQuery(DAGPipeline & pipeline);
    void executeImpl(DAGPipeline & pipeline);
    void executeTS(const tipb::TableScan & ts, DAGPipeline & pipeline);
    void executeJoin(const tipb::Join & join, DAGPipeline & pipeline, SubqueryForSet & right_query);
    void prepareJoin(const google::protobuf::RepeatedPtrField<tipb::Expr> & keys, const DataTypes & key_types, DAGPipeline & pipeline,
        Names & key_names, bool left, bool is_right_out_join, const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
        String & filter_column_name);
    ExpressionActionsPtr genJoinOtherConditionAction(const tipb::Join & join, std::vector<NameAndTypePair> & source_columns,
        String & filter_column_for_other_condition, String & filter_column_for_other_eq_condition);
    void executeWhere(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, String & filter_column);
    void executeExpression(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr);
    void executeOrder(DAGPipeline & pipeline, std::vector<NameAndTypePair> & order_columns);
    void executeLimit(DAGPipeline & pipeline);
    void executeAggregation(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, Names & aggregation_keys,
        TiDB::TiDBCollators & collators, AggregateDescriptions & aggregate_descriptions);
    void executeProject(DAGPipeline & pipeline, NamesWithAliases & project_cols);

    SortDescription getSortDescription(std::vector<NameAndTypePair> & order_columns);
    void recordProfileStreams(DAGPipeline & pipeline, const String & key);
<<<<<<< HEAD
=======
    bool addTimeZoneCastAfterTS(const BoolVec & is_ts_column, ExpressionActionsChain & chain);
>>>>>>> master

    void executeRemoteQueryImpl(DAGPipeline & pipeline, const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
        ::tipb::DAGRequest & dag_req, const DAGSchema & schema);

    Context & context;
    std::vector<BlockInputStreams> input_streams_vec;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;
    const tipb::DAGRequest & rqst;

    NamesWithAliases final_project;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    TableLockHolder table_drop_lock;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;


    std::vector<const tipb::Expr *> conditions;
    const DAGQuerySource & dag;
    std::vector<SubqueriesForSets> & subqueriesForSets;
    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map;
    BoolVec timestamp_column_flag_for_tablescan;

    Poco::Logger * log;
};
} // namespace DB
