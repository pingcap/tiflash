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
    DMFileBlockInputStream(const Context & context, const DMFilePtr & dmfile, const ColumnDefines & read_columns, const RSOperatorPtr & filter)
        : reader(dmfile,
                 read_columns,
                 filter,
                 context.getMarkCache().get(),
                 context.getMinMaxIndexCache().get(),
                 context.getSettingsRef().min_bytes_to_use_direct_io,
                 context.getSettingsRef().max_read_buffer_size)
    {
    }

    ~DMFileBlockInputStream() {}

    String getName() const override { return "DMFile"; }

    Block getHeader() const override { return reader.getHeader(); }

    size_t getSkippedRows() override { return reader.getSkippedRows(); }

    Block read() override { return reader.read(); }

private:
    DMFileReader reader;
};

} // namespace DM
} // namespace DB