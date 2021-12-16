#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Flash/Statistics/parseIdFromExecutorId.h>

namespace DB
{
Int64 parseIdFromExecutorId(const String & executor_id)
{
    auto split_index = executor_id.find('_');
    if (split_index == String::npos || split_index == (executor_id.size() - 1))
    {
        throw TiFlashException("Fail to parse id from executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }
    try
    {
        return std::stoi(executor_id.substr(split_index + 1, executor_id.size()));
    }
    catch (...)
    {
        throw TiFlashException("Fail to parse id from executor_id: " + executor_id, Errors::Coprocessor::Internal);
    }
}
} // namespace DB
