#include <Common/MyDuration.h>


namespace DB
{
std::tuple<Int32, Int32, Int32, Int32, Int32> MyDuration::splitDuration() const
{
    int sign = 1, hours, minutes, seconds, fraction;
    Int64 t = nanos;
    if (t < 0)
    {
        t = -t;
        sign = -1;
    }
    hours = t / HOUR;
    t -= hours * HOUR;
    minutes = t / MINUTE;
    t -= minutes * MINUTE;
    seconds = t / SECOND;
    t -= seconds * SECOND;
    fraction = t / MICRO_SECOND;
    return std::tuple<int, int, int, int, int>(sign, hours, minutes, seconds, fraction);
}

UInt32 MyDuration::hours() const
{
    auto [sign, hours, minutes, seconds, fraction] = splitDuration();
    return hours;
}

UInt32 MyDuration::minutes() const
{
    auto [sign, hours, minutes, seconds, fraction] = splitDuration();
    return minutes;
}

UInt32 MyDuration::seconds() const
{
    auto [sign, hours, minutes, seconds, fraction] = splitDuration();
    return seconds;
}

UInt32 MyDuration::microsecond() const
{
    auto [sign, hours, minutes, seconds, fraction] = splitDuration();
    return fraction;
}
} // namespace DB