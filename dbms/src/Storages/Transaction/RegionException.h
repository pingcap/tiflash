#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/Region.h>

namespace DB
{

 class RegionException : public Exception
{
public:
    explicit RegionException(std::vector<RegionID> region_ids_): region_ids(region_ids_) {}

     std::vector<RegionID> region_ids;
};

}
