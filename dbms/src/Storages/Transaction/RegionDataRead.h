#pragma once

#include <list>

#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

using RegionDataReadInfo = std::tuple<HandleID, UInt8, Timestamp, std::shared_ptr<const TiKVValue>>;

using RegionDataReadInfoList = std::vector<RegionDataReadInfo>;

} // namespace DB
