#include <Common/MyDuration.h>
#include <fmt/format.h>

namespace DB
{
DurationParts MyDuration::splitDuration() const
{
    Int64 sign = 1;
    Int64 t = nanos;
    if (t < 0)
    {
        t = -t;
        sign = -1;
    }
    Int64 hours = t / NANOS_PER_HOUR;
    t -= hours * NANOS_PER_HOUR;
    Int64 minutes = t / NANOS_PER_MINUTE;
    t -= minutes * NANOS_PER_MINUTE;
    Int64 seconds = t / NANOS_PER_SECOND;
    t -= seconds * NANOS_PER_SECOND;
    Int64 micro_seconds = t / NANOS_PER_MICRO;
    return DurationParts{sign, hours, minutes, seconds, micro_seconds};
}

Int32 MyDuration::hours() const
{
    return splitDuration().hour;
}

Int32 MyDuration::minutes() const
{
    return splitDuration().minute;
}

Int32 MyDuration::seconds() const
{
    return splitDuration().second;
}

Int32 MyDuration::microSecond() const
{
    return splitDuration().microsecond;
}

String MyDuration::toString() const
{
    auto [sign, hour, minute, second, microsecond] = splitDuration();
    if (fsp == 0)
    {
        return fmt::format("{}{:02}:{:02}:{:02}", sign > 0 ? "" : "-", hour, minute, second);
    }
    auto fmt_str = fmt::format("{}{}{}", "{}{:02}:{:02}:{:02}.{:.", fsp, "}");
    auto frac_str = fmt::format("{:06}", microsecond);
    return fmt::format(fmt_str, sign > 0 ? "" : "-", hour, minute, second, frac_str);
}
} // namespace DB