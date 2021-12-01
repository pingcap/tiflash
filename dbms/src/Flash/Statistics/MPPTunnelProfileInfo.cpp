#include <Flash/Statistics/MPPTunnelProfileInfo.h>
#include <fmt/format.h>

namespace DB
{
String MPPTunnelProfileInfo::toJson() const
{
    return fmt::format(
        R"({{"connection_type":"{}","packets":{},"bytes":{},"tunnel_id":"{}"}})",
        connection_type,
        blocks,
        bytes,
        tunnel_id);
}
} // namespace DB