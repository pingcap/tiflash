#pragma once

#include <Storages/Transaction/TiKVKeyValue.h>

#include <list>

namespace DB
{

using RegionDataReadInfo = std::tuple<RawTiDBPK, UInt8, Timestamp, std::shared_ptr<const TiKVValue>>;

using RegionDataReadInfoList = std::vector<RegionDataReadInfo>;

} // namespace DB
