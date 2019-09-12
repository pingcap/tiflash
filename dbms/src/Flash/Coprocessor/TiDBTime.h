#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>
#pragma GCC diagnostic pop

#include <Core/Types.h>
#include <common/DateLUT.h>
#include <common/DateLUTImpl.h>

namespace DB
{

class TiDBTime
{
public:
    TiDBTime(DayNum_t day_num, const DateLUTImpl & date_lut, const tipb::FieldType & field_type)
    {
        auto values = date_lut.getValues(day_num);
        year = values.year;
        month = values.month;
        day = values.day_of_month;
        hour = 0;
        minute = 0;
        second = 0;
        microsecond = 0;
        time_type = field_type.tp();
        fsp = field_type.decimal();
    }
    TiDBTime(time_t time_num, const DateLUTImpl & date_lut, const tipb::FieldType & field_type)
    {
        auto values = date_lut.getValues(time_num);
        year = values.year;
        month = values.month;
        day = values.day_of_month;
        hour = date_lut.toHour(time_num);
        minute = date_lut.toMinute(time_num);
        second = date_lut.toSecond(time_num);
        microsecond = 0;
        time_type = field_type.tp();
        fsp = field_type.decimal();
    }
    // When it's type is Time, HH:MM:SS may be 839:59:59, so use uint32 to avoid overflow.
    UInt32 hour; // hour <= 23
    UInt32 microsecond;
    UInt16 year;  // year <= 9999
    UInt8 month;  // month <= 12
    UInt8 day;    // day <= 31
    UInt8 minute; // minute <= 59
    UInt8 second; // second <= 59
    UInt8 time_type;
    Int8 fsp;
};
} // namespace DB
