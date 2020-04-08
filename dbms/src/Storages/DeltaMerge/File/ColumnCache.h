#pragma once

#include <Core/Block.h>
#include <common/logger_useful.h>
#include <cstddef>
#include <memory>
#include <Storages/Transaction/Types.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
namespace DM
{
using ColId = DB::ColumnID;
using PackRange = std::pair<size_t, size_t>;
using PackRanges = std::vector<PackRange>;
class ColumnCache : public std::enable_shared_from_this<ColumnCache>, private boost::noncopyable
{
public:
    enum class Strategy {
        Memory,
        Disk
    };

    ColumnCache() {}

    void putColumn(size_t pack_id, size_t pack_count, const ColumnPtr & column, ColId column_id);

    std::pair<PackRange, ColumnPtr> getColumn(const PackRange & range, ColId column_id);

    std::vector<std::pair<PackRange, ColumnCache::Strategy>> getReadStrategy(size_t pack_id, size_t pack_count, ColId column_id);

public:
    static std::shared_ptr<ColumnCache> null_cache;

private:
    void insertPackRange(size_t pack_id, size_t pack_count);

    static std::vector<PackRange> splitPackRangeByCacheRange(const PackRange & range, const PackRange & cache_range);

    static PackRange interleaveRange(const PackRange & range1, const PackRange & range2);

    static bool isRangeEmpty(const PackRange & range) {
        return range.second - range.first == 0;
    }

    static bool isSameRange(const PackRange & range1, const PackRange & range2) {
        return (range1.first == range2.first) && (range1.second == range2.second);
    }

private:
    std::vector<ColumnPtr>                 handle_columns;
    std::vector<ColumnPtr>                 version_columns;
    PackRanges pack_ranges;
};

using ColumnCachePtr = std::shared_ptr<ColumnCache>;
} // namespace DM
} // namespace DB
