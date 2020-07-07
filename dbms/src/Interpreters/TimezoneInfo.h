#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/TiDB.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

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
    void resetByDAGRequest(const tipb::DAGRequest & rqst)
    {
        if (rqst.has_time_zone_name() && !rqst.time_zone_name().empty())
        {
            // dag request use name based timezone info
            is_name_based = true;
            timezone_offset = 0;
            timezone = &DateLUT::instance(rqst.time_zone_name());
            timezone_name = timezone->getTimeZone();
            is_utc_timezone = timezone_name == "UTC";
        }
        else if (rqst.has_time_zone_offset())
        {
            // dag request use offset based timezone info
            is_name_based = false;
            timezone_offset = rqst.time_zone_offset();
            timezone = &DateLUT::instance("UTC");
            timezone_name = "";
            is_utc_timezone = timezone_offset == 0;
        }
        else
        {
            // dag request does not have timezone info
            is_name_based = false;
            timezone_offset = 0;
            // set the default timezone to UTC because TiDB assumes
            // the default timezone is UTC
            timezone = &DateLUT::instance("UTC");
            timezone_name = "";
            is_utc_timezone = true;
        }
    }
};

} // namespace DB
