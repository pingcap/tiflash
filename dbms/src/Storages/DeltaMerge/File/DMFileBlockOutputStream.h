#pragma once

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>

namespace DB
{
namespace DM
{
class DMFileBlockOutputStream
{
public:
    DMFileBlockOutputStream(const Context &       context,
                            const DMFilePtr &     dmfile,
                            const ColumnDefines & write_columns,
                            bool                  need_rate_limit = false)
        : writer(dmfile,
                 write_columns,
                 context.getSettingsRef().min_compress_block_size,
                 context.getSettingsRef().max_compress_block_size,
                 // context.chooseCompressionSettings(0, 0), TODO: should enable this, and make unit testes work.
                 CompressionSettings(CompressionMethod::LZ4),
                 context.getFileProvider(),
                 need_rate_limit ? context.getRateLimiter() : nullptr)
    {
    }

    struct BlockProperty
    {
        size_t not_clean_rows;
        size_t effective_num_rows;
    };

    void write(const Block & block, const BlockProperty & block_property)
    {
        writer.write(block, block_property.not_clean_rows, block_property.effective_num_rows);
    }

    void writePrefix() {}

    void writeSuffix() { writer.finalize(); }

private:
    DMFileWriter writer;
};

} // namespace DM
} // namespace DB
