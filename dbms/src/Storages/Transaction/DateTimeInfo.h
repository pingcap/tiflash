#pragma once

#include <Core/Types.h>
#include <common/DateLUTImpl.h>

namespace DB
{

class DateTimeInfo
// todo use time zone info in DAG request
{
public:
    DateTimeInfo(UInt32 ch_raw_data);
    DateTimeInfo(UInt64 ch_raw_data, const DateLUTImpl & date_lut);
    DateTimeInfo(UInt64 tidb_raw_data);
    UInt64 packedToUInt64();

    UInt16 year = 0;
    UInt8 month = 0;
    UInt8 day = 0;
    UInt8 hour = 0;
    UInt8 minute = 0;
    UInt8 second = 0;
    // todo add nanosecond
    //UInt64 nanosecond;
};
} // namespace DB
