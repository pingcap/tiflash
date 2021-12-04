#pragma once

#include <Flash/Coprocessor/DAGInterpreterBase.h>

namespace DB
{
/** build ch plan from dag request: dag executors -> ch plan
  */
class JoinInterpreter : public DAGInterpreterBase
{
public:
    JoinInterpreter(
        Context & context_,
        const std::vector<DAGPipelinePtr> & input_pipelines_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_,
        bool keep_session_timezone_info_,
        const DAGQuerySource & dag_,
        std::vector<SubqueriesForSets> & subqueries_for_sets_,
        const LogWithPrefixPtr & log_);

private:
    void executeImpl(DAGPipelinePtr & pipeline) override;
    void executeJoin(const tipb::Join & join, DAGPipelinePtr & pipeline, SubqueryForSet & right_query);
    void prepareJoin(
        const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
        const DataTypes & key_types,
        DAGPipeline & pipeline,
        Names & key_names,
        bool left,
        bool is_right_out_join,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
        String & filter_column_name);
    ExpressionActionsPtr genJoinOtherConditionAction(
        const tipb::Join & join,
        std::vector<NameAndTypePair> & source_columns,
        String & filter_column_for_other_condition,
        String & filter_column_for_other_eq_condition);

    std::vector<DAGPipelinePtr> input_pipelines;
    std::vector<SubqueriesForSets> & subqueries_for_sets;
};
} // namespace DB
