#pragma once

#include <functional>

#include <Storages/Transaction/PDTiKVClient.h>

namespace DB
{

using IndexReaderCreateFunc = std::function<IndexReaderPtr(pingcap::kv::RegionVerID)>;

} // namespace DB
