#pragma once

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB
{
namespace DM
{
class DMFileBlockInputStream : public SkippableBlockInputStream
{
public:
    DMFileBlockInputStream(const Context &       context,
                           UInt64                max_read_version,
                           bool                  enable_clean_read,
                           const DMFilePtr &     dmfile,
                           const ColumnDefines & read_columns,
                           const HandleRange &   handle_range,
                           const RSOperatorPtr & filter,
                           ColumnCachePtr &      column_cache_,
                           const IdSetPtr &      read_packs,
                           size_t                expected_size = DMFILE_READ_ROWS_THRESHOLD)
        : reader(dmfile,
                 read_columns,
                 // clean read
                 enable_clean_read,
                 max_read_version,
                 // filters
                 handle_range,
                 filter,
                 read_packs,
                 // caches
                 context.getGlobalContext().getMarkCache().get(),
                 context.getGlobalContext().getMinMaxIndexCache().get(),
                 context.getSettingsRef().dt_enable_stable_column_cache,
                 column_cache_,
                 context.getSettingsRef().min_bytes_to_use_direct_io,
                 context.getSettingsRef().max_read_buffer_size,
                 context.getFileProvider(),
                 expected_size)
    {
    }

    ~DMFileBlockInputStream() {}

    String getName() const override { return "DMFile"; }

    Block getHeader() const override { return reader.getHeader(); }

    bool getSkippedRows(size_t & skip_rows) override { return reader.getSkippedRows(skip_rows); }

    Block read() override { return reader.read(); }

private:
    DMFileReader reader;
};

} // namespace DM
} // namespace DB
