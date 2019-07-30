#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#include <kvproto/coprocessor.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Coprocessor/CoprocessorHandler.h>
#include "CoprocessorBuilderUtils.h"
#include "ExpressionActions.h"

namespace DB {

/** build ch plan from dag request: dag executors -> ch plan
  */
class InterpreterDagRequestV2 {
public:
    InterpreterDagRequestV2(CoprocessorContext & context_, tipb::DAGRequest & dag_request);

    ~InterpreterDagRequestV2();

    BlockIO execute();

private:
    CoprocessorContext & context;
    tipb::DAGRequest & dag_request;
    NamesWithAliases final_project;
    bool has_where;
    bool has_agg;
    bool has_orderby;
    bool has_limit;
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

        bool hasMoreThanOneStream() const
        {
            return streams.size() > 1;
        }
    };

    bool buildPlan(const tipb::Executor & executor, Pipeline & streams);
    bool buildTSPlan(const tipb::TableScan & ts, Pipeline & streams);

};
}
