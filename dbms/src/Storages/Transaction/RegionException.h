#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class RegionException : public Exception
{
public:
    enum RegionReadStatus : UInt8
    {
        OK,
        NOT_FOUND,
        VERSION_ERROR,
    };

    static const char * RegionReadStatusString(RegionReadStatus s)
    {
        switch (s)
        {
            case OK:
                return "OK";
            case NOT_FOUND:
                return "NOT_FOUND";
            case VERSION_ERROR:
                return "VERSION_ERROR";
        }
        return "Unknown";
    };

public:
    RegionException(std::vector<RegionID> && region_ids_, RegionReadStatus status_)
        : Exception(RegionReadStatusString(status_)), region_ids(region_ids_), status(status_)
    {}

    std::vector<RegionID> region_ids;
    RegionReadStatus status;
};

} // namespace DB
