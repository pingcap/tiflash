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
                           const CompressionSettings & compression_settings_)
    : dmfile(dmfile_),
      write_columns(write_columns_),
      min_compress_block_size(min_compress_block_size_),
      max_compress_block_size(max_compress_block_size_),
      compression_settings(compression_settings_)
{
    dmfile->setStatus(DMFile::Status::WRITING);
    for (auto & cd : write_columns)
    {
        bool do_index = cd.id == EXTRA_HANDLE_COLUMN_ID;
        column_streams.emplace(cd.id,
                               std::make_unique<Stream>(dmfile->colDataPath(cd.id), //
                                                        cd.type,
                                                        compression_settings,
                                                        max_compress_block_size,
                                                        do_index));
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
        auto & avg_size = dmfile->column_stats.at(cd.id).avg_size;
        IDataType::updateAvgValueSizeHint(*col, avg_size);
    }
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

    stream->marks->push_back(MarkInCompressedFile{stream->plain_hashing.count(), stream->original_hashing.offset()});

    type.serializeBinaryBulkWithMultipleStreams(column, //
                                                [&](const IDataType::SubstreamPath &) { return &(stream->original_hashing); },
                                                0,
                                                rows,
                                                true,
                                                {});

    stream->original_hashing.nextIfAtEnd();
}

void DMFileWriter::finalizeColumn(ColId col_id, const IDataType & type)
{
    auto   chunks = dmfile->getChunks();
    auto & stream = column_streams.at(col_id);
    stream->finalize();

    if (stream->minmaxes)
    {
        WriteBufferFromFile buf(dmfile->colIndexPath(col_id));
        stream->minmaxes->write(type, buf);
    }

    {
        if (unlikely(stream->marks->size() != chunks))
            throw Exception("Size of marks is expected to be " + DB::toString(chunks) + ", but " + DB::toString(stream->marks->size()));

        // TODO: don't need buffer here.
        WriteBufferFromFile buf(dmfile->colMarkPath(col_id));
        buf.write((char *)stream->marks->data(), sizeof(MarkInCompressedFile) * chunks);
    }
}

} // namespace DM
} // namespace DB