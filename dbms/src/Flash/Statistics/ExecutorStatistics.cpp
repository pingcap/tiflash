#include <Common/FmtUtils.h>
#include <Common/joinStr.h>
#include <Flash/Statistics/ExecutorStatistics.h>

namespace DB
{
void ExecutorStatistics::collectExecutorStatistics(DAGContext & dag_context)
{
    for (auto & profile_streams_info : dag_context.getProfileStreamsMap())
    {
        const auto & executor_id = profile_streams_info.first;
        if (AggStatistics::hit(executor_id))
        {
            agg_stats.push_back(AggStatistics::buildStatistics(executor_id, profile_streams_info.second, dag_context));
        }
        else if (FilterStatistics::hit(executor_id))
        {
            filter_stats.push_back(FilterStatistics::buildStatistics(executor_id, profile_streams_info.second, dag_context));
        }
    }
}

String ExecutorStatistics::toString() const
{
    FmtBuffer buffer;
    buffer.append(R"({"agg_stats":[)");
    joinStr(
        agg_stats.cbegin(),
        agg_stats.cend(),
        buffer,
        [](const auto & s, FmtBuffer & fb) { fb.append(s->toString()); },
        ",");
    buffer.append(R"(],"filter_stats":[)");
    joinStr(filter_stats.cbegin(), filter_stats.cend(), buffer, [](const auto & s, FmtBuffer & fb) { fb.append(s->toString()); });
    buffer.append("]}");
    return buffer.toString();
}
} // namespace DB