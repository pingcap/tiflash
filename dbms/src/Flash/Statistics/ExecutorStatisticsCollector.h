#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

#include <map>

namespace DB
{
class ExecutorStatisticsCollector
{
public:
    void initialize(DAGContext * dag_context_);

    void collectRuntimeDetails();

    const std::map<String, ExecutorStatisticsPtr> & getResult() const { return res; }

private:
    DAGContext * dag_context = nullptr;

    std::map<String, ExecutorStatisticsPtr> res;

    template <typename T>
    inline bool doAppend(const String & executor_id, const tipb::Executor * executor)
    {
        if (T::hit(executor_id))
        {
            res[executor_id] = std::make_shared<T>(executor, *dag_context);
            return true;
        }
        return false;
    }

    template <typename... Ts>
    inline bool append(const String & executor_id, const tipb::Executor * executor)
    {
        assert(res.find(executor_id) == res.end());
        return (doAppend<Ts>(executor_id, executor) || ...);
    }
};
} // namespace DB