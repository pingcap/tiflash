#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Coprocessor/CoprocessorHandler.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/CoprocessorBuilderUtils.h>
#include <Interpreters/DAGQueryInfo.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

/** build ch plan from dag request: dag executors -> ch plan
  */
class InterpreterDAGRequest : public IInterpreter
{
public:
    InterpreterDAGRequest(CoprocessorContext & context_, DAGQueryInfo & dag_query_info);

    ~InterpreterDAGRequest() = default;

    BlockIO execute();

private:
    CoprocessorContext & context;
    NamesWithAliases final_project;
    DAGQueryInfo & dag_query_info;
    NamesAndTypesList source_columns;
    size_t max_streams = 1;

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
    };

    bool executeImpl(Pipeline & pipeline);
    bool executeTS(const tipb::TableScan & ts, Pipeline & pipeline);
    void executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, String & filter_column);
    void executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr);
    void executeOrder(Pipeline & pipeline, Strings & order_column_names);
    void executeUnion(Pipeline & pipeline);
    void executeLimit(Pipeline & pipeline);
    void executeFinalProject(Pipeline & pipeline);
    SortDescription getSortDescription(Strings & order_column_names);
    AnalysisResult analyzeExpressions();
};
} // namespace DB
