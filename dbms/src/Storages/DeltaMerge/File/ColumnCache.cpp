#include <Storages/DeltaMerge/File/ColumnCache.h>

namespace DB
{
namespace DM
{
ColumnCachePtr ColumnCache::null_cache = std::make_shared<ColumnCache>();

void ColumnCache::putHandleColumn(size_t pack_id, size_t pack_count, const ColumnPtr & handle_column)
{
    handle_columns.push_back(handle_column);
    insertPackRange(pack_id, pack_count);
}

std::pair<PackRange, ColumnPtr> ColumnCache::tryGetHandleColumn(size_t pack_id, size_t pack_count)
{
    PackRange target{pack_id, pack_count};
    PackRange result{0, 0};
    for (size_t i = 0; i < pack_ranges.size(); i++)
    {
        result = interleaveRange(target, pack_ranges[i]);
        if (result.second != 0 && i < handle_columns.size())
        {
            return std::make_pair(pack_ranges[i], handle_columns[i]);
        }
    }

    return {{0, 0}, nullptr};
}

void ColumnCache::putVersionColumn(size_t pack_id, size_t pack_count, const ColumnPtr & handle_column)
{
    version_columns.push_back(handle_column);
    insertPackRange(pack_id, pack_count);
}

std::pair<PackRange, ColumnPtr> ColumnCache::tryGetVersionColumn(size_t pack_id, size_t pack_count)
{
    PackRange target{pack_id, pack_count};
    PackRange result{0, 0};
    for (size_t i = 0; i < pack_ranges.size(); i++)
    {
        result = interleaveRange(target, pack_ranges[i]);
        if (result.second != 0 && i < version_columns.size())
        {
            return std::make_pair(pack_ranges[i], version_columns[i]);
        }
    }

    return {{0, 0}, nullptr};
}

PackRange ColumnCache::interleaveRange(const PackRange & range1, const PackRange & range2)
{
    if (range1.first >= range2.second || range2.first >= range1.second)
    {
        return PackRange(0, 0);
    }
    size_t start = std::max(range1.first, range2.first);
    size_t end   = std::min(range1.second, range2.second);
    return PackRange(start, end);
}

std::vector<PackRange> ColumnCache::splitPackRangeByCacheRange(const PackRange & range, size_t start, size_t end)
{
    std::vector<PackRange> results;
    size_t                 current = start;
    if (range.first > start)
    {
        results.emplace_back(PackRange(current, range.first));
        current = range.first;
    }
    if (range.second < end)
    {
        results.emplace_back(PackRange(current, range.second));
        current = range.second;
    }
    results.emplace_back(PackRange(current, end));

    return results;
}

void ColumnCache::insertPackRange(size_t pack_id, size_t pack_count)
{
    if (pack_ranges.empty())
    {
        pack_ranges.emplace_back(std::make_pair(pack_id, pack_id + pack_count));
    }
    else
    {
        auto range = pack_ranges.back();
        if (range.first != pack_id)
        {
            pack_ranges.emplace_back(std::make_pair(pack_id, pack_id + pack_count));
        }
    }
}
} // namespace DM
} // namespace DB