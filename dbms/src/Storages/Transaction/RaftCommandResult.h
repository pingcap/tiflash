#pragma once

#include <variant>

#include <Storages/Transaction/Types.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;

struct IndexError
{
};

struct BatchSplit
{
    std::vector<RegionPtr> split_regions;
};

struct UpdateTableID
{
    TableIDSet table_ids;
};

struct DefaultResult
{
};

struct ChangePeer
{
};

struct RaftCommandResult
{
    // UInt64 term;
    // UInt64 index;
    bool sync_log;
    std::variant<DefaultResult, IndexError, BatchSplit, UpdateTableID, ChangePeer> inner;
};

template <class... Fs>
struct overload : Fs...
{
    using Fs::operator()...;
};
template <class... Fs>
overload(Fs...)->overload<Fs...>;

} // namespace DB
