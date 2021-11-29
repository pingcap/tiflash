#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <fmt/format.h>

namespace DB
{
String ConnectionProfileInfo::toJson() const
{
    return fmt::format(
        R"({{"connection_type":"{}","packets":{},"bytes":{}{}}})",
        connection_type,
        packets,
        bytes,
        extraToJson());
}
} // namespace DB