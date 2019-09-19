#pragma once

#include <Storages/DeltaMerge/DeltaMergeStore.h>

namespace DB
{
namespace DM
{

/// Helper functions for locating which segment to write block / delete_range

DeltaMergeStore::WriteActions prepareWriteActions(const Block &                             block,
                                                  const DeltaMergeStore::SegmentSortedMap & segments,
                                                  const String &                            handle_name,
                                                  std::shared_lock<std::shared_mutex> &&    segments_read_lock);

DeltaMergeStore::WriteActions prepareWriteActions(const HandleRange &                       delete_range,
                                                  const DeltaMergeStore::SegmentSortedMap & segments,
                                                  std::shared_lock<std::shared_mutex> &&    segments_read_lock);


} // namespace DM
} // namespace DB
