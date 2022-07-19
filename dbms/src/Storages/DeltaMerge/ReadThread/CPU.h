#pragma once

#include <Poco/Logger.h>

#include <vector>

namespace DB::DM
{
// `getNumaNodes` returns cpus of each Numa node.
std::vector<std::vector<int>> getNumaNodes(Poco::Logger * log);
} // namespace DB::DM