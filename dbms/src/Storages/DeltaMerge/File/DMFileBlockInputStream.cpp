#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>

namespace DB::DM
{
DMFileBlockInputStreamBuilder::DMFileBlockInputStreamBuilder(const Context & context)
    : file_provider(context.getFileProvider())
    , read_limiter(context.getReadLimiter())
{
    // init from global context
    const auto & global_context = context.getGlobalContext();
    setCaches(global_context.getMarkCache(), global_context.getMinMaxIndexCache());
    // init from settings
    setFromSettings(context.getSettingsRef());
}

DMFileBlockInputStreamPtr DMFileBlockInputStreamBuilder::build(const DMFilePtr & dmfile, const ColumnDefines & read_columns, const RowKeyRanges & rowkey_ranges)
{
    if (dmfile->getStatus() != DMFile::Status::READABLE)
        throw Exception(fmt::format(
                            "DMFile [{}] is expected to be in READABLE status, but: {}",
                            dmfile->fileId(),
                            DMFile::statusString(dmfile->getStatus())),
                        ErrorCodes::LOGICAL_ERROR);

    // if `rowkey_ranges` is empty, we unconditionally read all packs
    // `rowkey_ranges` and `is_common_handle`  will only be useful in clean read mode.
    // It is safe to ignore them here.
    if (unlikely(rowkey_ranges.empty() && enable_clean_read))
        throw Exception("rowkey ranges shouldn't be empty with clean-read enabled", ErrorCodes::LOGICAL_ERROR);

    bool is_common_handle = !rowkey_ranges.empty() && rowkey_ranges[0].is_common_handle;

    DMFilePackFilter pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        index_cache,
        true,
        rowkey_ranges,
        rs_filter,
        read_packs,
        file_provider,
        read_limiter,
        tracing_logger);

    DMFileReader reader(
        dmfile,
        read_columns,
        is_common_handle,
        enable_clean_read,
        max_data_version,
        std::move(pack_filter),
        mark_cache,
        enable_column_cache,
        column_cache,
        aio_threshold,
        max_read_buffer_size,
        file_provider,
        read_limiter,
        rows_threshold_per_read,
        read_one_pack_every_time,
        tracing_logger);

    return std::make_shared<DMFileBlockInputStream>(std::move(reader));
}
} // namespace DB::DM
