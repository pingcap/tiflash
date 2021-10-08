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
    return std::get<1>(splitDuration());
}

UInt32 MyDuration::minutes() const
{
    return std::get<2>(splitDuration());
}

UInt32 MyDuration::seconds() const
{
    return std::get<3>(splitDuration());
}

UInt32 MyDuration::microsecond() const
{
    return std::get<4>(splitDuration());
}
} // namespace DB