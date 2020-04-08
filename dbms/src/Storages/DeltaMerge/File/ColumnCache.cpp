#include <Storages/DeltaMerge/File/ColumnCache.h>

namespace DB
{
namespace DM
{
ColumnCachePtr ColumnCache::null_cache = std::make_shared<ColumnCache>();

void ColumnCache::putColumn(size_t pack_id, size_t pack_count, const ColumnPtr & column, ColId column_id)
{
    if (column_id == EXTRA_HANDLE_COLUMN_ID) {
        handle_columns.push_back(column);
    } else if (column_id == VERSION_COLUMN_ID) {
        version_columns.push_back(column);
    } else {
        throw Exception("Unknown column id " + std::to_string(column_id), ErrorCodes::LOGICAL_ERROR);
    }
    insertPackRange(pack_id, pack_count);
}

std::pair<PackRange, ColumnPtr> ColumnCache::getColumn(const PackRange & target_range, ColId column_id)
{
    for (size_t i = 0; i < pack_ranges.size(); i++)
    {
        if (isSameRange(target_range, pack_ranges[i])) {
            if (column_id == EXTRA_HANDLE_COLUMN_ID) {
                return std::make_pair(pack_ranges[i], handle_columns[i]);
            } else if (column_id == VERSION_COLUMN_ID) {
                return std::make_pair(pack_ranges[i], version_columns[i]);
            } else {
                throw Exception("Unknown column id " + std::to_string(column_id), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
    return {{0, 0}, nullptr};
}

PackRange ColumnCache::interleaveRange(const PackRange & range1, const PackRange & range2)
{
    if (range1.second >= range2.first || range2.second >= range1.first)
    {
        return PackRange(0, 0);
    }
    size_t start = std::max(range1.first, range2.first);
    size_t end   = std::min(range1.second, range2.second);
    return PackRange(start, end);
}

std::vector<PackRange> ColumnCache::splitPackRangeByCacheRange(const PackRange & range, const PackRange & cache_range)
{
    std::vector<PackRange> results;
    size_t                 current = range.first;
    if (cache_range.first > current)
    {
        results.emplace_back(PackRange(current, cache_range.first));
        current = cache_range.first;
    }
    if (cache_range.second < range.second)
    {
        results.emplace_back(PackRange(current, cache_range.second));
        current = cache_range.second;
    }
    results.emplace_back(PackRange(current, range.second));
    return results;
}

void ColumnCache::insertPackRange(size_t pack_id, size_t pack_count)
{
    PackRange target_range{pack_id, pack_id + pack_count};
    if (pack_ranges.empty())
    {
        pack_ranges.emplace_back(std::make_pair(pack_id, pack_id + pack_count));
    }
    else
    {
        auto range = pack_ranges.back();
        if (!isSameRange(target_range, range)) {
            pack_ranges.emplace_back(target_range);
        }
    }
}

std::vector<std::pair<PackRange, ColumnCache::Strategy>> ColumnCache::getReadStrategy(size_t pack_id, size_t pack_count, ColId column_id) {
    PackRange target_range{pack_id, pack_id + pack_count};
    std::vector<std::pair<PackRange, ColumnCache::Strategy>> range_and_strategy;
    bool hit_cache = false;
    for (size_t i = 0; i < pack_ranges.size(); i++)
    {
        auto cache_range = interleaveRange(target_range, pack_ranges[i]);
        if (!isRangeEmpty(cache_range))
        {
            hit_cache = true;
            if (column_id == EXTRA_HANDLE_COLUMN_ID) {
                if (i == handle_columns.size()) {
                    range_and_strategy.emplace_back(std::make_pair(target_range, Strategy::Disk));
                } else {
                    auto ranges = splitPackRangeByCacheRange(target_range, cache_range);
                    for (auto & range : ranges) {
                        if (isSameRange(range, cache_range)) {
                            range_and_strategy.emplace_back(std::make_pair(range, Strategy::Memory));
                        } else {
                            range_and_strategy.emplace_back(std::make_pair(range, Strategy::Disk));
                        }
                    }
                }
            } else if (column_id == VERSION_COLUMN_ID) {
                if (i == version_columns.size()) {
                    range_and_strategy.emplace_back(std::make_pair(target_range, Strategy::Disk));
                } else {
                    auto ranges = splitPackRangeByCacheRange(target_range, cache_range);
                    for (auto & range : ranges) {
                        if (isSameRange(range, cache_range)) {
                            range_and_strategy.emplace_back(std::make_pair(range, Strategy::Memory));
                        } else {
                            range_and_strategy.emplace_back(std::make_pair(range, Strategy::Disk));
                        }
                    }
                }
            } else {
                throw Exception("Unknown column id " + std::to_string(column_id), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
    if (!hit_cache) {
        range_and_strategy.emplace_back(std::make_pair(target_range, Strategy::Disk));
    }
    return range_and_strategy;
}
} // namespace DM
} // namespace DB