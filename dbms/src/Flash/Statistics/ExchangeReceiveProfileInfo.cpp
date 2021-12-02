#include <Flash/Statistics/ExchangeReceiveProfileInfo.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeReceiveProfileInfo::extraToJson() const
{
    return fmt::format(
        R"(,"partition_id":{},"receiver_source_task_id":{})",
        partition_id,
        receiver_source_task_id);
}
} // namespace DB