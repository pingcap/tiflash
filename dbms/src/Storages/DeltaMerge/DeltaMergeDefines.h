#pragma once

#include <limits>
#include <memory>

#include <Common/Allocator.h>
#include <Common/ArenaWithFreeLists.h>

#include <Core/Types.h>

namespace DB
{
/// S: Scale factor of delta tree node.
/// M: Capacity factor of leaf node.
/// F: Capacity factor of Intern node.
/// For example, the max capacity of leaf node would be: M * S.
static constexpr size_t DT_S = 3;
static constexpr size_t DT_M = 55;
static constexpr size_t DT_F = 13;

extern const size_t INVALID_ID;

template <class ValueSpace, size_t M, size_t F, size_t S = 3, typename TAllocator = Allocator<false>>
class DeltaTree;

template <size_t M, size_t F, size_t S>
class DTEntryIterator;

class MemoryValueSpace;

using EntryIterator    = DTEntryIterator<DT_M, DT_F, DT_S>;
using ValueSpacePtr    = std::shared_ptr<MemoryValueSpace>;
using DefaultDeltaTree = DeltaTree<MemoryValueSpace, DT_M, DT_F, DT_S, ArenaWithFreeLists>;
using DeltaTreePtr     = std::shared_ptr<DefaultDeltaTree>;
using Ids              = std::vector<UInt64>;

} // namespace DB