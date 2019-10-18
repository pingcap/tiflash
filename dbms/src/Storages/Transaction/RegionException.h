#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class RegionException : public Exception
{
public:
    RegionException(std::vector<RegionID> && region_ids_, RegionTable::RegionReadStatus status_)
        : Exception(RegionTable::RegionReadStatusString(status_)), region_ids(region_ids_), status(status_)
    {}

    std::vector<RegionID> region_ids;
    RegionTable::RegionReadStatus status;
};

} // namespace DB
