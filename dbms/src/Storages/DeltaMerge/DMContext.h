#pragma once

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{

class StoragePathPool;

class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

namespace DM
{

class StoragePool;
using NotCompress = std::unordered_set<ColId>;
struct DMContext;
using DMContextPtr = std::shared_ptr<DMContext>;

/**
 * This context object carries table infos. And those infos are only meaningful to current context.
 */
struct DMContext : private boost::noncopyable
{
    const Context &         db_context;
    const TiFlashMetricsPtr metrics;

    StoragePathPool & path_pool;
    StoragePool &     storage_pool;
    const UInt64      hash_salt;

    // gc safe-point, maybe update.
    DB::Timestamp min_version;

    const NotCompress & not_compress; // Not used currently.

    bool is_common_handle;

    // The number of columns in primary key if is_common_handle = true, otherwise, should always be 1.
    size_t rowkey_column_size;

    // The base rows of segment.
    const size_t segment_limit_rows;
    // The base bytes of segment.
    const size_t segment_limit_bytes;
    // The rows threshold of delta.
    const size_t delta_limit_rows;
    // The rows threshold of delta.
    const size_t delta_limit_bytes;
    // The threshold of cache in delta.
    const size_t delta_cache_limit_rows;
    // The size threshold of cache in delta.
    const size_t delta_cache_limit_bytes;
    // Determine whether a pack is small or not in rows.
    const size_t delta_small_pack_rows;
    // Determine whether a pack is small or not in bytes.
    const size_t delta_small_pack_bytes;
    // The expected stable pack rows.
    const size_t stable_pack_rows;

    // The number of points to check for calculating region split.
    const size_t region_split_check_points = 128;

    const bool enable_logical_split;
    const bool read_delta_only;
    const bool read_stable_only;
    const bool enable_relevant_place;
    const bool enable_skippable_place;

    const String query_id;

public:
    DMContext(const Context &      db_context_,
              StoragePathPool &    path_pool_,
              StoragePool &        storage_pool_,
              const UInt64         hash_salt_,
              const DB::Timestamp  min_version_,
              const NotCompress &  not_compress_,
              bool                 is_common_handle_,
              size_t               rowkey_column_size_,
              const DB::Settings & settings,
              const String &       query_id_ = "")
        : db_context(db_context_),
          metrics(db_context.getTiFlashMetrics()),
          path_pool(path_pool_),
          storage_pool(storage_pool_),
          hash_salt(hash_salt_),
          min_version(min_version_),
          not_compress(not_compress_),
          is_common_handle(is_common_handle_),
          rowkey_column_size(rowkey_column_size_),
          segment_limit_rows(settings.dt_segment_limit_rows),
          segment_limit_bytes(settings.dt_segment_limit_size),
          delta_limit_rows(settings.dt_segment_delta_limit_rows),
          delta_limit_bytes(settings.dt_segment_delta_limit_size),
          delta_cache_limit_rows(settings.dt_segment_delta_cache_limit_rows),
          delta_cache_limit_bytes(settings.dt_segment_delta_cache_limit_size),
          delta_small_pack_rows(settings.dt_segment_delta_small_pack_rows),
          delta_small_pack_bytes(settings.dt_segment_delta_small_pack_size),
          stable_pack_rows(settings.dt_segment_stable_pack_rows),
          enable_logical_split(settings.dt_enable_logical_split),
          read_delta_only(settings.dt_read_delta_only),
          read_stable_only(settings.dt_read_stable_only),
          enable_relevant_place(settings.dt_enable_relevant_place),
          enable_skippable_place(settings.dt_enable_skippable_place),
          query_id(query_id_)
    {
    }

};

} // namespace DM
} // namespace DB
