#pragma once

#include <Storages/Transaction/Types.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;

struct RaftCommandResult
{
    enum Type
    {
        Default,
        IndexError,
        BatchSplit,
        UpdateTableID,
        ChangePeer
    };

    bool sync_log;

    Type type = Type::Default;
    std::vector<RegionPtr> split_regions{};
    TableIDSet table_ids{};
};

} // namespace DB
