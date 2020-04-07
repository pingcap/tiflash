#pragma once

#include <Core/Block.h>
#include <common/logger_useful.h>
#include <cstddef>
#include <memory>

namespace DB
{
namespace DM
{
using PackRange = std::pair<size_t, size_t>;
class ColumnCache : public std::enable_shared_from_this<ColumnCache>, private boost::noncopyable
{
public:
    ColumnCache() {}
    void putHandleColumn(size_t pack_id, size_t pack_count, const ColumnPtr & handle_column);

    std::pair<PackRange, ColumnPtr> tryGetHandleColumn(size_t pack_id, size_t pack_count);

    void putVersionColumn(size_t pack_id, size_t pack_count, const ColumnPtr & handle_column);

    std::pair<PackRange, ColumnPtr> tryGetVersionColumn(size_t pack_id, size_t pack_count);

public:
    static std::shared_ptr<ColumnCache> null_cache;

    static std::vector<PackRange> splitPackRangeByCacheRange(const PackRange & range, size_t start, size_t end);

private:
    void insertPackRange(size_t pack_id, size_t pack_count);

private:
    static PackRange interleaveRange(const PackRange & range1, const PackRange & range2);

private:
    std::vector<ColumnPtr>                 handle_columns;
    std::vector<ColumnPtr>                 version_columns;
    std::vector<std::pair<size_t, size_t>> pack_ranges;
};

using ColumnCachePtr = std::shared_ptr<ColumnCache>;
} // namespace DM
} // namespace DB
