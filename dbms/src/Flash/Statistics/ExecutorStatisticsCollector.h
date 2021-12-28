#pragma once

#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <tipb/executor.pb.h>

#include <map>

namespace DB
{
class DAGContext;

class ExecutorStatisticsCollector
{
public:
    void initialize(DAGContext * dag_context_);

    void collectRuntimeDetails();

    const std::map<String, ExecutorStatisticsPtr> & getResult() const { return res; }

    String resToJson() const;

    DAGContext & getDAGContext() const;

private:
    DAGContext * dag_context = nullptr;

    std::map<String, ExecutorStatisticsPtr> res;

    template <typename T>
    bool doAppend(const String & executor_id, const tipb::Executor * executor)
    {
        if (T::isMatch(executor))
        {
            res[executor_id] = std::make_shared<T>(executor, *dag_context);
            return true;
        }
        return false;
    }

    template <typename... Ts>
    bool append(const String & executor_id, const tipb::Executor * executor)
    {
        assert(res.find(executor_id) == res.end());
        return (doAppend<Ts>(executor_id, executor) || ...);
    }
};
} // namespace DB