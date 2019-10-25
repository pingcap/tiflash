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
};
} // namespace DB
