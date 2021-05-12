#pragma once

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>

namespace DB
{
namespace DM
{

/// The output stream for writing block to DTFile.
///
/// Note that we will filter block by `RSOperatorPtr` while reading, so the
/// blocks output to DTFile must be bounded by primary key, or we will get
/// wrong results by filtering.
/// You can use `PKSquashingBlockInputStream` to reorganize the boundary of
/// blocks.
class DMFileBlockOutputStream
{
public:
    using Flags = DMFileWriter::Flags;

    DMFileBlockOutputStream(const Context &       context,
                            const DMFilePtr &     dmfile,
                            const ColumnDefines & write_columns,
                            const Flags           flags = Flags())
        : writer(
            dmfile,
            write_columns,
            context.getFileProvider(),
            flags.needRateLimit() ? context.getRateLimiter() : nullptr,
            DMFileWriter::Options{
                CompressionMethod::LZ4, // context.chooseCompressionSettings(0, 0), TODO: should enable this, and make unit testes work.
                context.getSettingsRef().min_compress_block_size,
                context.getSettingsRef().max_compress_block_size,
                flags})
    {
    }

    const DMFilePtr getFile() const { return writer.getFile(); }

    using BlockProperty = DMFileWriter::BlockProperty;
    void write(const Block & block, const BlockProperty & block_property) { writer.write(block, block_property); }

    void writePrefix() {}

    void writeSuffix() { writer.finalize(); }

private:
    DMFileWriter writer;
};

} // namespace DM
} // namespace DB
