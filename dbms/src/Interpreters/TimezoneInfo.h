#pragma once

#include <Core/Types.h>
#include <common/DateLUT.h>

namespace tipb
{
class DAGRequest;
}

namespace DB
{

/// A class used to store timezone info, currently only used when handling coprocessor request
struct TimezoneInfo
{
    String timezone_name;
    Int64 timezone_offset;
    bool is_utc_timezone;
    bool is_name_based;
    const DateLUTImpl * timezone;

    void init()
    {
        is_name_based = true;
        timezone_offset = 0;
        timezone = &DateLUT::instance();
        timezone_name = timezone->getTimeZone();
        is_utc_timezone = timezone_name == "UTC";
    }

    void resetByDAGRequest(const tipb::DAGRequest & rqst);
};

} // namespace DB
