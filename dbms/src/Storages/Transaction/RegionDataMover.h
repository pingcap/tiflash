#pragma once

#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class StorageMergeTree;
class Context;

using HandleMap = std::unordered_map<HandleID, std::tuple<UInt64, UInt8>>;

template <typename HandleType>
HandleMap getHandleMapByRange(Context & context, StorageMergeTree & storage, const HandleRange<HandleType> & handle_range);

} // namespace DB
