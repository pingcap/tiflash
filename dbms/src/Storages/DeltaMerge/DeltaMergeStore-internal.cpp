#include <Storages/DeltaMerge/DeltaMergeStore-internal.h>

namespace DB
{
namespace DM
{

DeltaMergeStore::WriteActions prepareWriteActions(const Block &                             block,
                                                  const DeltaMergeStore::SegmentSortedMap & segments,
                                                  const String &                            handle_name,
                                                  std::shared_lock<std::shared_mutex>       segments_read_lock)
{
    (void)segments_read_lock;

    DeltaMergeStore::WriteActions actions;

    const size_t rows        = block.rows();
    size_t       offset      = 0;
    const auto & handle_data = getColumnVectorData<Handle>(block, block.getPositionByName(handle_name));
    while (offset != rows)
    {
        auto start      = handle_data[offset];
        auto segment_it = segments.upper_bound(start);
        if (segment_it == segments.end())
        {
            if (start == P_INF_HANDLE)
                --segment_it;
            else
                throw Exception("Failed to locate segment begin with start: " + DB::toString(start), ErrorCodes::LOGICAL_ERROR);
        }
        auto segment = segment_it->second;
        auto range   = segment->getRange();
        auto end_pos = range.end == P_INF_HANDLE ? handle_data.cend()
                                                 : std::lower_bound(handle_data.cbegin() + offset, handle_data.cend(), range.end);
        size_t limit = end_pos - (handle_data.cbegin() + offset);

        actions.emplace_back(DeltaMergeStore::WriteAction{.segment = segment, .offset = offset, .limit = limit});

        offset += limit;
    }

    return actions;
}

DeltaMergeStore::WriteActions prepareWriteActions(const HandleRange &                       delete_range,
                                                  const DeltaMergeStore::SegmentSortedMap & segments,
                                                  std::shared_lock<std::shared_mutex>       segments_read_lock)
{
    (void)segments_read_lock;

    DeltaMergeStore::WriteActions actions;

    for (auto & [handle_, segment] : segments)
    {
        (void)handle_;
        if (segment->getRange().intersect(delete_range))
        {
            // TODO maybe more precise on `action.update`
            actions.emplace_back(DeltaMergeStore::WriteAction{.segment = segment, .offset = 0, .limit = 0, .update = delete_range});
        }
    }

    return actions;
}

} // namespace DM
} // namespace DB
