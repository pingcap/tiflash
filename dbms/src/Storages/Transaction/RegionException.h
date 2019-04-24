#pragma once

#include <Common/Exception.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

// TODO REVIEW: exception code?
 class RegionException : public Exception
{
public:
    explicit RegionException(std::vector<RegionID> region_ids_): region_ids(region_ids_) {}

     std::vector<RegionID> region_ids;
};

}
