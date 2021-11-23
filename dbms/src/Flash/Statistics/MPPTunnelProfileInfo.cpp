#include <Flash/Statistics/MPPTunnelProfileInfo.h>
#include <fmt/format.h>

namespace DB
{
String MPPTunnelProfileInfo::toJson() const
{
    return fmt::format(
        R"({{"connection_type":"{}","rows":{},"blocks":{},"bytes":{},"tunnel_id":"{}"}})",
        connection_type,
        rows,
        blocks,
        bytes,
        tunnel_id);
}
} // namespace DB