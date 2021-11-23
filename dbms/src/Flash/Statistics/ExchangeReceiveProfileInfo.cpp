#include <Flash/Statistics/ExchangeReceiveProfileInfo.h>
#include <fmt/format.h>

namespace DB
{
String ExchangeReceiveProfileInfo::toJson() const
{
    return fmt::format(
        R"({{"connection_type":"{}","rows":{},"blocks":{},"bytes":{},"partition_id":{},"sender_task_id":{}}})",
        connection_type,
        rows,
        blocks,
        bytes,
        partition_id,
        sender_task_id);
}
} // namespace DB