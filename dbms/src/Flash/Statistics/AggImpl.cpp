#include <Common/TiFlashException.h>
#include <Flash/Statistics/AggImpl.h>

namespace DB
{
void AggStatistics::appendExtraJson(FmtBuffer &) const
{
}

void AggStatistics::collectExtraRuntimeDetail()
{}

AggStatistics::AggStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : AggStatisticsBase(executor, dag_context_)
{}
} // namespace DB