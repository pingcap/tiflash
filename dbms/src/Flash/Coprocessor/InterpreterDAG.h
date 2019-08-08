#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IInterpreter.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

class Context;

/** build ch plan from dag request: dag executors -> ch plan
  */
class InterpreterDAG : public IInterpreter
{
public:
    InterpreterDAG(Context & context_, const DAGQuerySource & dag_);

    ~InterpreterDAG() = default;

    BlockIO execute();

private:
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

    void executeImpl(Pipeline & pipeline);
    void executeTS(const tipb::TableScan & ts, Pipeline & pipeline);
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
    void recordProfileStreams(Pipeline & pipeline, Int32 index);

private:
    Context & context;

    const DAGQuerySource & dag;

    NamesWithAliases final_project;
    NamesAndTypesList source_columns;

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;

    /// Table from where to read data, if not subquery.
    TMTStoragePtr storage;
    TableStructureReadLockPtr table_lock;

    Poco::Logger * log;
};
} // namespace DB
