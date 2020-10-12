#pragma once

#include <Storages/DeltaMerge/Range.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{

void sortRangesByStartEdge(HandleRanges & ranges);

HandleRanges tryMergeRanges(HandleRanges && ranges, size_t expected_ranges_count, Logger * log = nullptr);

} // namespace DM
} // namespace DB