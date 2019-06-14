#pragma once

#include <Core/Types.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>

namespace DB
{

using NotCompress = std::unordered_set<ColId>;

class StoragePool;

/**
 * This context object carry table infos. And the infos are only meaningful to current context.
 */
struct DMContext
{
    const Context & db_context;
    StoragePool &   storage_pool;

    const String &        table_name;
    const ColumnDefines & table_columns;
    const ColumnDefine &  table_handle_define;

    const UInt64 min_version;

    const NotCompress & not_compress;

    // The threshold of delta.
    const size_t delta_limit_rows;
    const size_t delta_limit_bytes;

    // The threshold of cache in delta.
    const size_t delta_cache_limit_rows;
    const size_t delta_cache_limit_bytes;
};
} // namespace DB