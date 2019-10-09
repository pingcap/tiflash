#pragma once

#include <Core/Types.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>

namespace DB
{
namespace DM
{

using NotCompress = std::unordered_set<ColId>;

class StoragePool;

/**
 * This context object carries table infos. And those infos are only meaningful to current context.
 */
struct DMContext
{
    const Context & db_context;
    StoragePool &   storage_pool;

    // The schema snapshot
    // We need a consistent snapshot of columns, copy ColumnsDefines
    const ColumnDefines store_columns;
    const ColumnDefine  handle_column;

    const UInt64 min_version;

    const NotCompress & not_compress;

    // The rows of segment.
    const size_t segment_rows;

    // The threshold of delta.
    const size_t delta_limit_rows;
    const size_t delta_limit_bytes;

    // The threshold of cache in delta.
    const size_t delta_cache_limit_rows;
    const size_t delta_cache_limit_bytes;
};

using DMContextPtr = std::shared_ptr<DMContext>;

} // namespace DM
} // namespace DB