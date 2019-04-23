#pragma once

#include <list>

#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

using RegionDataReadInfo = std::tuple<HandleID, UInt8, Timestamp, TiKVValue>;

using RegionDataReadInfoList = std::list<RegionDataReadInfo>;

} // namespace DB
