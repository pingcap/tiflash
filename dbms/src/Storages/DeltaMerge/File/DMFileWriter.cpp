#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/Index/MinMax.h>

namespace DB
{
namespace DM
{

DMFileWriter::DMFileWriter(const DMFilePtr &           dmfile_,
                           const ColumnDefines &       write_columns_,
                           size_t                      min_compress_block_size_,
                           size_t                      max_compress_block_size_,
                           const CompressionSettings & compression_settings_,
                           bool                        wal_mode_)
    : dmfile(dmfile_),
      write_columns(write_columns_),
      min_compress_block_size(min_compress_block_size_),
      max_compress_block_size(max_compress_block_size_),
      compression_settings(compression_settings_),
      wal_mode(wal_mode_),
      split_file(dmfile->splitPath())
{
    dmfile->setStatus(DMFile::Status::WRITING);
    for (auto & cd : write_columns)
    {
        // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.

        bool do_index = !wal_mode && (cd.type->isInteger() || cd.type->isDateOrDateTime());
        auto stream   = std::make_unique<Stream>(dmfile, //
                                               cd.id,
                                               cd.type,
                                               compression_settings,
                                               max_compress_block_size,
                                               do_index);
        column_streams.emplace(cd.id, std::move(stream));
        dmfile->column_stats.emplace(cd.id, ColumnStat{cd.id, cd.type, /*avg_size=*/0});
    }
}

void DMFileWriter::write(const Block & block)
{
    size_t rows = block.rows();
    for (auto & cd : write_columns)
    {
        auto & col = getByColumnId(block, cd.id).column;
        writeColumn(cd.id, *cd.type, *col);
    }
    writeIntBinary(rows, split_file);
    if (wal_mode)
        split_file.sync();

    dmfile->addChunk(rows);
}

void DMFileWriter::finalize()
{
    for (auto & cd : write_columns)
    {
        finalizeColumn(cd.id, *(cd.type));
    }

    dmfile->finalize();
}

void DMFileWriter::writeColumn(ColId col_id, const IDataType & type, const IColumn & column)
{
    size_t rows   = column.size();
    auto & stream = column_streams.at(col_id);
    if (stream->minmaxes)
        stream->minmaxes->addChunk(column, nullptr);

    /// There could already be enough data to compress into the new block.
    if (stream->original_hashing.offset() >= min_compress_block_size)
        stream->original_hashing.next();

    auto offset_in_compressed_block = stream->original_hashing.offset();
    if (unlikely(wal_mode && offset_in_compressed_block != 0))
        throw Exception("Offset in compressed block is expected to be 0, now " + DB::toString(offset_in_compressed_block));

    writeIntBinary(stream->plain_hashing.count(), stream->mark_file);
    writeIntBinary(offset_in_compressed_block, stream->mark_file);

    type.serializeBinaryBulkWithMultipleStreams(column, //
                                                [&](const IDataType::SubstreamPath &) { return &(stream->original_hashing); },
                                                0,
                                                rows,
                                                true,
                                                {});

    if (wal_mode)
        stream->flush();
    else
        stream->original_hashing.nextIfAtEnd();

    auto & avg_size = dmfile->column_stats.at(col_id).avg_size;
    IDataType::updateAvgValueSizeHint(column, avg_size);
}

void DMFileWriter::finalizeColumn(ColId col_id, const IDataType & type)
{
    auto & stream = column_streams.at(col_id);
    stream->flush();

    if (stream->minmaxes)
    {
        WriteBufferFromFile buf(dmfile->colIndexPath(col_id));
        stream->minmaxes->write(type, buf);
    }
}

} // namespace DM
} // namespace DB