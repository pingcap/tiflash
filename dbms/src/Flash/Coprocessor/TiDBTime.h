#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>
#pragma GCC diagnostic pop

#include <Common/MyTime.h>
#include <Core/Types.h>

namespace DB
{
class TiDBTime
{
public:
    TiDBTime(UInt64 packed, const tipb::FieldType & field_type)
        : my_date_time(packed)
    {
        time_type = field_type.tp();
        fsp = field_type.decimal();
        fsp_tt_date = packed & MyTimeBase::FSPTT_BIT_FIELD_MASK;
    }
    MyDateTime my_date_time;
    UInt8 time_type;
    Int8 fsp;
    UInt8 fsp_tt_date;
    UInt64 toChunkTime() const
    {
        UInt64 ret = 0;
        ret |= (my_date_time.toCoreTime() & MyTimeBase::CORE_TIME_BIT_FIELD_MASK);
        if (time_type == TiDB::TypeDate || fsp_tt_date == MyTimeBase::FSPTT_FOR_DATE)
        {
            ret |= MyTimeBase::FSPTT_FOR_DATE;
            return ret;
        }

        Int8 realFsp = fsp;

        if (my_date_time.micro_second <= UInt32(999999) && my_date_time.micro_second > UInt32(0))
        {
            realFsp = static_cast<Int8>(6);
        }

        if (realFsp > 0)
            ret |= UInt64(realFsp) << 1u;

        if (time_type == TiDB::TypeTimestamp || time_type == TiDB::TypeDatetime)
            ret |= 1u;
        return ret;
    }
};
} // namespace DB
