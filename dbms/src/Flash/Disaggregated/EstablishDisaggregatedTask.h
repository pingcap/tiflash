#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Executor/QueryExecutorHolder.h>
#include <kvproto/mpp.pb.h>
#include <tipb/select.pb.h>

#include <memory>

namespace DB
{
class Context;
using ContextPtr = std::shared_ptr<Context>;
class EstablishDisaggregatedTask;
using EstablishDisaggregatedTaskPtr = std::shared_ptr<EstablishDisaggregatedTask>;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

class EstablishDisaggregatedTask
{
public:
    explicit EstablishDisaggregatedTask(ContextPtr context_);

    void prepare(const mpp::EstablishDisaggregatedTaskRequest * request);

    void execute(mpp::EstablishDisaggregatedTaskResponse * response);

private:
    ContextPtr context;
    tipb::DAGRequest dag_req;
    std::unique_ptr<DAGContext> dag_context;
    QueryExecutorHolder query_executor_holder;

    const LoggerPtr log;
};
} // namespace DB
