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
struct DMContext : private boost::noncopyable
{
    const Context & db_context;
    String          store_path;
    StoragePool &   storage_pool;

    // The schema snapshot
    // We need a consistent snapshot of columns, copy ColumnsDefines
    const ColumnDefines store_columns;
    const ColumnDefine  handle_column;

    // gc safe-point, maybe update.
    DB::Timestamp min_version;

    const NotCompress & not_compress;

    // The rows of segment.
    const size_t segment_limit_rows;
    // The threshold of delta.
    const size_t delta_limit_rows;
    // The threshold of cache in delta.
    const size_t delta_cache_limit_rows;
    // The expected stable chunk rows.
    const size_t stable_chunk_rows;

    DMContext(const Context &       db_context_,
              const String &        store_path_,
              StoragePool &         storage_pool_,
              const ColumnDefines & store_columns_,
              const ColumnDefine &  handle_column_,
              const UInt64          min_version_,
              const NotCompress &   not_compress_,
              const size_t          segment_limit_rows_,
              const size_t          delta_limit_rows_,
              const size_t          delta_cache_limit_rows_,
              const size_t          stable_chunk_rows_)
        : db_context(db_context_),
          store_path(store_path_),
          storage_pool(storage_pool_),
          store_columns(store_columns_),
          handle_column(handle_column_),
          min_version(min_version_),
          not_compress(not_compress_),
          segment_limit_rows(segment_limit_rows_),
          delta_limit_rows(delta_limit_rows_),
          delta_cache_limit_rows(delta_cache_limit_rows_),
          stable_chunk_rows(stable_chunk_rows_)
    {
    }
};

using DMContextPtr = std::shared_ptr<DMContext>;

} // namespace DM
} // namespace DB