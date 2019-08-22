#include <Storages/Transaction/DateTimeInfo.h>

#include <common/LocalDate.h>

namespace DB
{
DateTimeInfo::DateTimeInfo(UInt32 ch_date_raw_data)
{
    DayNum_t day_num(ch_date_raw_data);
    LocalDate local_date(day_num);
    year = local_date.year();
    month = local_date.month();
    day = local_date.day();
    hour = minute = second = 0;
}
DateTimeInfo::DateTimeInfo(Int64 ch_datetime_raw_data, const DateLUTImpl & date_lut)
{
    time_t date_time = ch_datetime_raw_data;
    const auto & values = date_lut.getValues(date_time);
    year = values.year;
    month = values.month;
    day = values.day_of_month;
    hour = date_lut.toHour(date_time);
    minute = date_lut.toMinute(date_time);
    second = date_lut.toSecond(date_time);
}
DateTimeInfo::DateTimeInfo(UInt64 tidb_raw_data)
{
    UInt64 ymdhms = tidb_raw_data >> 24;
    UInt64 ymd = ymdhms >> 17;
    day = UInt8(ymd & ((1 << 5) - 1));
    UInt64 ym = ymd >> 5;
    month = UInt8(ym % 13);
    year = UInt16(ym / 13);

    UInt64 hms = ymdhms & ((1 << 17) - 1);
    second = UInt8(hms & ((1 << 6) - 1));
    minute = UInt8((hms >> 6) & ((1 << 6) - 1));
    hour = UInt8(hms >> 12);
}

UInt64 DateTimeInfo::packedToUInt64()
{
    UInt64 ymd = ((UInt64)year * 13 + month) << 5 | day;
    UInt64 hms = (UInt64)hour << 12 | minute << 6 | second;
    return (ymd << 17 | hms) << 24;
}
} // namespace DB
