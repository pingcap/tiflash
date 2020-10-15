#pragma once

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{

void sortRangesByStartEdge(RowKeyRanges & ranges);

RowKeyRanges tryMergeRanges(RowKeyRanges && ranges, size_t expected_ranges_count, Logger * log = nullptr);

} // namespace DM
} // namespace DB