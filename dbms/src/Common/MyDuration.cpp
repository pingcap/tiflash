#include <Common/MyDuration.h>


namespace DB
{
MyDuration::MyDuration(Int64 packed)
{
    sign = 1;
    if (packed < 0)
    {
        sign = -1;
        packed = -packed;
    }
    hour = packed / HOUR;
    packed -= hour * HOUR;
    minute = packed / MINUTE;
    packed -= minute * MINUTE;
    second = packed / SECOND;
    packed -= second * SECOND;
    micro_second = packed / MICRO_SECOND;
}

MyDuration::MyDuration(UInt8 sign_, Int16 hour_, UInt8 minute_, UInt8 second_, UInt32 micro_second_)
    : sign(sign_)
    , hour(hour_)
    , minute(minute_)
    , second(second_)
    , micro_second(micro_second_)
{
}
} // namespace DB