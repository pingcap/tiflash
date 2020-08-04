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
                           const FileProviderPtr &     file_provider_,
                           bool                        wal_mode_)
    : dmfile(dmfile_),
      write_columns(write_columns_),
      min_compress_block_size(min_compress_block_size_),
      max_compress_block_size(max_compress_block_size_),
      compression_settings(compression_settings_),
      wal_mode(wal_mode_),
      pack_stat_file(file_provider_, dmfile->packStatPath(), dmfile->encryptionPackStatPath(), true),
      file_provider(file_provider_)
{
    dmfile->setStatus(DMFile::Status::WRITING);
    for (auto & cd : write_columns)
    {
        // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.
        // TODO: If column type is nullable, we won't generate index for it
        bool do_index = !wal_mode && (cd.type->isInteger() || cd.type->isDateOrDateTime());
        addStreams(cd.id, cd.type, do_index);
        dmfile->column_stats.emplace(cd.id, ColumnStat{cd.id, cd.type, /*avg_size=*/0});
    }
}

void DMFileWriter::addStreams(ColId col_id, DataTypePtr type, bool do_index)
{
    auto callback = [&](const IDataType::SubstreamPath & substream_path) {
        String stream_name = DMFile::getFileNameBase(col_id, substream_path);
        auto   stream      = std::make_unique<Stream>(dmfile, //
                                               stream_name,
                                               type,
                                               compression_settings,
                                               max_compress_block_size,
                                               file_provider,
                                               IDataType::isNullMap(substream_path) ? false : do_index);
        column_streams.emplace(stream_name, std::move(stream));
    };

    type->enumerateStreams(callback, {});
}

void DMFileWriter::write(const Block & block, size_t not_clean_rows)
{
    DMFile::PackStat stat;
    stat.rows      = block.rows();
    stat.not_clean = not_clean_rows;
    stat.bytes     = block.bytes(); // This is bytes of pack data in memory.

    for (auto & cd : write_columns)
    {
        auto & col = getByColumnId(block, cd.id).column;
        writeColumn(cd.id, *cd.type, *col);

        if (cd.id == VERSION_COLUMN_ID)
            stat.first_version = col->get64(0);
        else if (cd.id == TAG_COLUMN_ID)
            stat.first_tag = (UInt8)(col->get64(0));
    }

    writePODBinary(stat, pack_stat_file);
    if (wal_mode)
        pack_stat_file.sync();

    dmfile->addPack(stat);
}

void DMFileWriter::finalize()
{
    for (auto & cd : write_columns)
    {
        finalizeColumn(cd.id, *(cd.type));
    }

    dmfile->finalize(file_provider);
}

void DMFileWriter::writeColumn(ColId col_id, const IDataType & type, const IColumn & column)
{
    size_t rows = column.size();

    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream) {
            String name   = DMFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(name);
            if (stream->minmaxes)
                stream->minmaxes->addPack(column, nullptr);

            /// There could already be enough data to compress into the new block.
            if (stream->original_hashing.offset() >= min_compress_block_size)
                stream->original_hashing.next();

            auto offset_in_compressed_block = stream->original_hashing.offset();
            if (unlikely(wal_mode && offset_in_compressed_block != 0))
                throw Exception("Offset in compressed block is expected to be 0, now " + DB::toString(offset_in_compressed_block));

            writeIntBinary(stream->plain_hashing.count(), stream->mark_file);
            writeIntBinary(offset_in_compressed_block, stream->mark_file);
        },
        {});

    type.serializeBinaryBulkWithMultipleStreams(column, //
                                                [&](const IDataType::SubstreamPath & substream) {
                                                    String stream_name = DMFile::getFileNameBase(col_id, substream);
                                                    auto & stream      = column_streams.at(stream_name);
                                                    return &(stream->original_hashing);
                                                },
                                                0,
                                                rows,
                                                true,
                                                {});

    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream) {
            String name   = DMFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(name);
            if (wal_mode)
                stream->flush();
            else
                stream->original_hashing.nextIfAtEnd();
        },
        {});

    auto & avg_size = dmfile->column_stats.at(col_id).avg_size;
    IDataType::updateAvgValueSizeHint(column, avg_size);
}

void DMFileWriter::finalizeColumn(ColId col_id, const IDataType & type)
{
    size_t bytes_written = 0;
    auto   callback      = [&](const IDataType::SubstreamPath & substream) {
        String stream_name = DMFile::getFileNameBase(col_id, substream);
        auto & stream      = column_streams.at(stream_name);
        stream->flush();
        bytes_written += stream->getWrittenBytes();

        if (stream->minmaxes)
        {
            WriteBufferFromFileProvider buf(
                file_provider, dmfile->colIndexPath(stream_name), dmfile->encryptionIndexPath(stream_name), false);
            stream->minmaxes->write(type, buf);
            buf.sync();
            bytes_written += buf.getPositionInFile();
        }
    };

    type.enumerateStreams(callback, {});

    // Update column's bytes in disk
    dmfile->column_stats.at(col_id).serialized_bytes = bytes_written;
}

} // namespace DM
} // namespace DB
