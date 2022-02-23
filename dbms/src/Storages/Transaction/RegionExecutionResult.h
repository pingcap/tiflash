#pragma once

#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/Types.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace kvrpcpb
{
class LockInfo;
} // namespace kvrpcpb

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
        ChangePeer,
        CommitMerge,
    };

    bool sync_log;

    Type type = Type::Default;
    std::vector<RegionPtr> split_regions{};
    ImutRegionRangePtr ori_region_range;
    RegionID source_region_id;
};

struct RegionMergeResult
{
    bool source_at_left;
    UInt64 version;
};

} // namespace DB
