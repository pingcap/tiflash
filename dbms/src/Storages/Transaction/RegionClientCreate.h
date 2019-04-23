#pragma once

#include <functional>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tikv/RegionClient.h>
#pragma GCC diagnostic pop

namespace DB
{

using RegionClientCreateFunc = std::function<pingcap::kv::RegionClientPtr(pingcap::kv::RegionVerID)>;

} // namespace DB
