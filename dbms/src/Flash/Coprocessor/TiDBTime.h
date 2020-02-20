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
    TiDBTime(UInt64 packed, const tipb::FieldType & field_type) : my_date_time(packed)
    {
        time_type = field_type.tp();
        fsp = field_type.decimal();
    }
    MyDateTime my_date_time;
    UInt8 time_type;
    Int8 fsp;
    UInt64 toChunkTime() const
    {
        UInt64 ret = 0;
        ret |= (my_date_time.toCoreTime() & MyTimeBase::CORE_TIME_BIT_FIELD_MASK);
        if (time_type == TiDB::TypeDate)
        {
            ret |= MyTimeBase::FSPTT_FOR_DATE;
            return ret;
        }
        if (fsp > 0)
            ret |= UInt64(fsp) << 1u;
        if (time_type == TiDB::TypeTimestamp)
            ret |= 1u;
        return ret;
    }
};
} // namespace DB
