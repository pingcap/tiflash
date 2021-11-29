#include <Flash/Statistics/MPPTunnelProfileInfo.h>
#include <fmt/format.h>

namespace DB
{
String MPPTunnelProfileInfo::extraToJson() const
{
    return fmt::format(
        R"(,"tunnel_id":"{}","sender_target_task_id":{})",
        tunnel_id,
        sender_target_task_id);
}
} // namespace DB