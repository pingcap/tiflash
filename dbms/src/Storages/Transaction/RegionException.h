#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class RegionException : public Exception
{
public:
    // - region not found : This region does not exist or has been removed.
    // - region epoch not match : This region may has executed split/merge/change-peer command.
    enum class RegionReadStatus : UInt8
    {
        OK,
        NOT_FOUND,
        EPOCH_NOT_MATCH,
    };

    static const char * RegionReadStatusString(RegionReadStatus s)
    {
        switch (s)
        {
            case RegionReadStatus::OK:
                return "OK";
            case RegionReadStatus::NOT_FOUND:
                return "NOT_FOUND";
            case RegionReadStatus::EPOCH_NOT_MATCH:
                return "EPOCH_NOT_MATCH";
        }
        return "Unknown";
    };

public:
    RegionException(std::vector<RegionID> && region_ids_, RegionReadStatus status_, RegionID unavailable_region_ = InvalidRegionID)
        : Exception(RegionReadStatusString(status_)), region_ids(region_ids_), status(status_), unavailable_region(unavailable_region_)
    {}

    std::vector<RegionID> region_ids;
    RegionReadStatus status;
    /// Region could be found with correct epoch, but unavailable (e.g. its lease in proxy has not been built with leader).
    RegionID unavailable_region;
};

} // namespace DB
