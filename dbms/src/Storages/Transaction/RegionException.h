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

    using UnavailableRegions = std::unordered_set<RegionID>;

public:
    RegionException(UnavailableRegions && unavailable_region_, RegionReadStatus status_)
        : Exception(RegionReadStatusString(status_)), unavailable_region(std::move(unavailable_region_)), status(status_)
    {}

    /// Region could be found with correct epoch, but unavailable (e.g. its lease in proxy has not been built with leader).
    UnavailableRegions unavailable_region;
    RegionReadStatus status;
};

} // namespace DB
