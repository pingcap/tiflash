#pragma once

#include <vector>

namespace DB::DM
{
// `getNumaNodes` returns cpus of each Numa node.
std::vector<std::vector<int>> getNumaNodes();
}