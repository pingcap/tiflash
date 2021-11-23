#include <Flash/Statistics/LocalReadProfileInfo.h>
#include <fmt/format.h>

namespace DB
{
String LocalReadProfileInfo::toJson() const
{
    return fmt::format(
        R"({{"connection_type":"{}","rows":{},"blocks":{},"bytes":{}}})",
        connection_type,
        rows,
        blocks,
        bytes);
}
} // namespace DB