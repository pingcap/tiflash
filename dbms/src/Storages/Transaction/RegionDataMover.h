#pragma once

#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

class StorageMergeTree;
class Context;

using HandleMap = std::unordered_map<HandleID, std::tuple<UInt64, UInt8>>;

template <typename HandleType>
void getHandleMapByRange(Context &, StorageMergeTree &, const HandleRange<HandleType> &, HandleMap &);

void tryOptimizeStorageFinal(Context &, TableID);

} // namespace DB
