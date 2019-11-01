#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/Index/MinMax.h>

namespace DB
{
namespace DM
{

DMFileWriter::DMFileWriter(const DMFilePtr &           dmfile_,
                           const ColumnDefines &       col_defs_,
                           size_t                      min_compress_block_size_,
                           size_t                      max_compress_block_size_,
                           const CompressionSettings & compression_settings_)
    : dmfile(dmfile_),
      col_defs(col_defs_),
      min_compress_block_size(min_compress_block_size_),
      max_compress_block_size(max_compress_block_size_),
      compression_settings(compression_settings_)
{
    for (auto & cd : col_defs)
    {
        auto stream = std::make_shared<Stream>(dmfile->colDataPath(cd.id), cd.type, compression_settings, max_compress_block_size);
        column_streams.emplace(cd.id, stream);
    }
}

void DMFileWriter::write(const Block & block)
{
    size_t rows = block.rows();
    for (auto & cd : col_defs)
    {
        auto & col = getByColumnId(block, cd.id).column;
        writeColumn(cd.id, *cd.type, *col);
    }
    total_rows += rows;
}

void DMFileWriter::writeColumn(ColId col_id, const IDataType & type, const IColumn & column)
{
    size_t rows   = column.size();
    auto & stream = column_streams.at(col_id);
    if (col_id == EXTRA_HANDLE_COLUMN_ID)
    {
        auto [min_index, max_index] = minmaxVec<Handle>(column, nullptr, 0, rows);
        stream->minmaxes->insertFrom(column, min_index);
        stream->minmaxes->insertFrom(column, max_index);
    }

    /// There could already be enough data to compress into the new block.
    if (stream->compressed_hashing.offset() >= min_compress_block_size)
        stream->compressed_hashing.next();

    stream->marks->push_back(stream->plain_hashing.count());
    stream->marks->push_back(stream->compressed_hashing.offset());

    type.serializeBinaryBulkWithMultipleStreams(column, //
                                                [&](const IDataType::SubstreamPath &) { return &(stream->compressed_hashing); },
                                                0,
                                                rows,
                                                true,
                                                {});

    stream->compressed_hashing.nextIfAtEnd();
}

void DMFileWriter::finalize()
{

}

} // namespace DM
} // namespace DB