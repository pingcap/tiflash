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
    DMFileBlockInputStream(const Context &        context,
                           UInt64                 max_read_version,
                           bool                   enable_clean_read,
                           UInt64                 hash_salt,
                           const DMFilePtr &      dmfile,
                           const ColumnDefines &  read_columns,
                           const RowKeyRange &    rowkey_range,
                           const RSOperatorPtr &  filter,
                           const ColumnCachePtr & column_cache_,
                           const IdSetPtr &       read_packs,
                           size_t                 expected_size             = DMFILE_READ_ROWS_THRESHOLD,
                           bool                   read_one_pack_every_time_ = false)
        : reader(dmfile,
                 read_columns,
                 // clean read
                 enable_clean_read,
                 max_read_version,
                 // filters
                 rowkey_range,
                 filter,
                 read_packs,
                 // caches
                 hash_salt,
                 context.getGlobalContext().getMarkCache(),
                 context.getGlobalContext().getMinMaxIndexCache(),
                 context.getSettingsRef().dt_enable_stable_column_cache,
                 column_cache_,
                 context.getSettingsRef().min_bytes_to_use_direct_io,
                 context.getSettingsRef().max_read_buffer_size,
                 context.getFileProvider(),
                 expected_size,
                 read_one_pack_every_time_)
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

using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;

} // namespace DM
} // namespace DB
