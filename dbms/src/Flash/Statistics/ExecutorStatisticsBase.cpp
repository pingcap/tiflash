#include <DataStreams/BlockStreamProfileInfo.h>
#include <Flash/Statistics/ExecutorStatisticsBase.h>

namespace DB
{
void BaseRuntimeStatistics::append(const BlockStreamProfileInfo & profile_info)
{
    rows += profile_info.rows;
    blocks += profile_info.blocks;
    bytes += profile_info.bytes;
    execution_time_ns = std::max(execution_time_ns, profile_info.execution_time);
}
} // namespace DB