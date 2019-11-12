#pragma once

#include <memory>

#include <Storages/Transaction/Types.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;

class RegionRangeKeys;
using ImutRegionRangePtr = std::shared_ptr<const RegionRangeKeys>;

struct RaftCommandResult : private boost::noncopyable
{
    enum Type
    {
        Default,
        IndexError,
        BatchSplit,
        UpdateTable,
        ChangePeer,
        CompactLog,
    };

    bool sync_log;

    Type type = Type::Default;
    std::vector<RegionPtr> split_regions{};
    ImutRegionRangePtr range_before_split;
};

} // namespace DB
