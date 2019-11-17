#pragma once

#include <Interpreters/Context.h>
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
                           UInt64                hash_salt,
                           const DMFilePtr &     dmfile,
                           const ColumnDefines & read_columns,
                           const RSOperatorPtr & filter,
                           const IdSetPtr &      read_chunks,
                           size_t                expected_size = DMFILE_READ_ROWS_THRESHOLD)
        : reader(dmfile,
                 read_columns,
                 filter,
                 read_chunks,
                 context.getGlobalContext().getMarkCache().get(),
                 context.getGlobalContext().getMinMaxIndexCache().get(),
                 hash_salt,
                 context.getSettingsRef().min_bytes_to_use_direct_io,
                 context.getSettingsRef().max_read_buffer_size,
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