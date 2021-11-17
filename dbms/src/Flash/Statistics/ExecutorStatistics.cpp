#include <Common/TiFlashException.h>
#include <Flash/Statistics/ExecutorStatistics.h>

namespace DB
{
ExecutorStatistics::ExecutorStatistics(const String & executor_id)
{
    auto split_index = executor_id.find('_');
    if (split_index == String::npos || split_index == (executor_id.size() - 1))
    {
        throw TiFlashException("Illegal executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }

    type = executor_id.substr(0, split_index);
    id = executor_id.substr(split_index + 1, executor_id.size());
}
} // namespace DB