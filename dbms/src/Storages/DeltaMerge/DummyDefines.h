#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{

class MemoryValueSpace;

using MyValueSpacePtr = std::shared_ptr<MemoryValueSpace>;
using MyDeltaTree    = DeltaTree<MemoryValueSpace, DT_M, DT_F, DT_S, ArenaWithFreeLists>;
using MyDeltaTreePtr = std::shared_ptr<MyDeltaTree>;

} // namespace DB