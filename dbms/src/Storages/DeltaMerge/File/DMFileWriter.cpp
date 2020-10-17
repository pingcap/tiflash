#include <Common/TiFlashException.h>
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
                           bool                        single_file_mode_)
    : dmfile(dmfile_),
      write_columns(write_columns_),
      min_compress_block_size(min_compress_block_size_),
      max_compress_block_size(max_compress_block_size_),
      compression_settings(compression_settings_),
      // assume pack_stat_file is the first file created inside DMFile
      // it will create encryption info for the whole DMFile
      pack_stat_file(
          single_file_mode_
              ? nullptr
              : createWriteBufferFromFileBaseByFileProvider(
                  file_provider_, dmfile->packStatPath(), dmfile->encryptionPackStatPath(), true, 0, 0, max_compress_block_size)),
      single_file_stream(
          !single_file_mode_ ? nullptr : new SingleFileStream(dmfile_, compression_settings_, max_compress_block_size_, file_provider_)),
      file_provider(file_provider_),
      single_file_mode(single_file_mode_)
{
    dmfile->setStatus(DMFile::Status::WRITING);
    for (auto & cd : write_columns)
    {
        // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.
        // TODO: If column type is nullable, we won't generate index for it
        /// for handle column always generate index
        bool do_index = cd.id == EXTRA_HANDLE_COLUMN_ID || cd.type->isInteger() || cd.type->isDateOrDateTime();

        if (single_file_mode)
        {
            if (do_index)
            {
                const auto column_name = DMFile::getFileNameBase(cd.id, {});
                single_file_stream->minmax_indexs.emplace(column_name, std::make_shared<MinMaxIndex>(*cd.type));
            }

            auto callback = [&](const IDataType::SubstreamPath & substream_path) {
                const auto stream_name = DMFile::getFileNameBase(cd.id, substream_path);
                single_file_stream->column_data_sizes.emplace(stream_name, 0);
                single_file_stream->column_mark_with_sizes.emplace(stream_name, SingleFileStream::MarkWithSizes{});
            };
            cd.type->enumerateStreams(callback, {});
        }
        else
        {
            addStreams(cd.id, cd.type, do_index);
        }
        dmfile->column_stats.emplace(cd.id, ColumnStat{cd.id, cd.type, /*avg_size=*/0});
    }
}

void DMFileWriter::addStreams(ColId col_id, DataTypePtr type, bool do_index)
{
    auto callback = [&](const IDataType::SubstreamPath & substream_path) {
        const auto stream_name = DMFile::getFileNameBase(col_id, substream_path);
        auto       stream      = std::make_unique<Stream>(dmfile, //
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

    auto del_mark_column = tryGetByColumnId(block, TAG_COLUMN_ID).column;

    const ColumnVector<UInt8> * del_mark = !del_mark_column ? nullptr : (const ColumnVector<UInt8> *)del_mark_column.get();

    for (auto & cd : write_columns)
    {
        auto & col = getByColumnId(block, cd.id).column;
        writeColumn(cd.id, *cd.type, *col, del_mark);

        if (cd.id == VERSION_COLUMN_ID)
            stat.first_version = col->get64(0);
        else if (cd.id == TAG_COLUMN_ID)
            stat.first_tag = (UInt8)(col->get64(0));
    }

    if (!single_file_mode)
    {
        writePODBinary(stat, *pack_stat_file);
    }

    dmfile->addPack(stat);
}

void DMFileWriter::finalize()
{
    for (auto & cd : write_columns)
    {
        finalizeColumn(cd.id, cd.type);
    }

    if (single_file_mode)
    {
        dmfile->finalize(single_file_stream->plain_hashing);
        single_file_stream->flush();
    }
    else
    {
        dmfile->finalize(file_provider);
    }
}

void DMFileWriter::writeColumn(ColId col_id, const IDataType & type, const IColumn & column, const ColumnVector<UInt8> * del_mark)
{
    size_t rows = column.size();

    if (single_file_mode)
    {
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            size_t     offset_in_compressed_file = single_file_stream->plain_hashing.count();
            const auto stream_name               = DMFile::getFileNameBase(col_id, substream);
            if (unlikely(substream.size() > 1))
                throw DB::TiFlashException("Substream_path shouldn't be more than one.", Errors::DeltaTree::Internal);

            auto & minmax_indexs = single_file_stream->minmax_indexs;
            if (auto iter = minmax_indexs.find(stream_name); iter != minmax_indexs.end())
            {
                iter->second->addPack(column, del_mark);
            }

            auto offset_in_compressed_block = single_file_stream->original_hashing.offset();
            if (unlikely(offset_in_compressed_block != 0))
                throw DB::TiFlashException("Offset in compressed block is always expected to be 0 when single_file_mode is true, now "
                                               + DB::toString(offset_in_compressed_block),
                                           Errors::DeltaTree::Internal);

            // write column data
            if (substream.empty())
            {
                if (unlikely(type.isNullable()))
                    throw DB::TiFlashException("Type shouldn't be nullable when substream_path is empty.", Errors::DeltaTree::Internal);

                type.serializeBinaryBulk(column, single_file_stream->original_hashing, 0, rows);
            }
            else if (substream[0].type == IDataType::Substream::NullMap)
            {
                if (unlikely(!type.isNullable()))
                    throw DB::TiFlashException("Type shouldn be nullable when substream_path's type is NullMap.",
                                               Errors::DeltaTree::Internal);

                const ColumnNullable & col = static_cast<const ColumnNullable &>(column);
                col.checkConsistency();
                DataTypeUInt8().serializeBinaryBulk(col.getNullMapColumn(), single_file_stream->original_hashing, 0, rows);
            }
            else if (substream[0].type == IDataType::Substream::NullableElements)
            {
                if (unlikely(!type.isNullable()))
                    throw DB::TiFlashException("Type shouldn be nullable when substream_path's type is NullableElements.",
                                               Errors::DeltaTree::Internal);

                const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
                const ColumnNullable &   col           = static_cast<const ColumnNullable &>(column);
                nullable_type.getNestedType()->serializeBinaryBulk(col.getNestedColumn(), single_file_stream->original_hashing, 0, rows);
            }
            else
            {
                throw DB::TiFlashException("Unknown type of substream_path: " + std::to_string(substream[0].type),
                                           Errors::DeltaTree::Internal);
            }
            single_file_stream->flushCompressedData();
            size_t mark_size_in_file = single_file_stream->plain_hashing.count() - offset_in_compressed_file;
            single_file_stream->column_mark_with_sizes.at(stream_name)
                .push_back(MarkWithSizeInCompressedFile{MarkInCompressedFile{.offset_in_compressed_file    = offset_in_compressed_file,
                                                                             .offset_in_decompressed_block = offset_in_compressed_block},
                                                        mark_size_in_file});
            single_file_stream->column_data_sizes[stream_name] += mark_size_in_file;
        };
        type.enumerateStreams(callback, {});
    }
    else
    {
        type.enumerateStreams(
            [&](const IDataType::SubstreamPath & substream) {
                const auto name   = DMFile::getFileNameBase(col_id, substream);
                auto &     stream = column_streams.at(name);
                if (stream->minmaxes)
                    stream->minmaxes->addPack(column, del_mark);

                /// There could already be enough data to compress into the new block.
                if (stream->original_hashing.offset() >= min_compress_block_size)
                    stream->original_hashing.next();

                auto offset_in_compressed_block = stream->original_hashing.offset();

                writeIntBinary(stream->plain_hashing.count(), stream->mark_file);
                writeIntBinary(offset_in_compressed_block, stream->mark_file);
            },
            {});

        type.serializeBinaryBulkWithMultipleStreams(column, //
                                                    [&](const IDataType::SubstreamPath & substream) {
                                                        const auto stream_name = DMFile::getFileNameBase(col_id, substream);
                                                        auto &     stream      = column_streams.at(stream_name);
                                                        return &(stream->original_hashing);
                                                    },
                                                    0,
                                                    rows,
                                                    true,
                                                    {});

        type.enumerateStreams(
            [&](const IDataType::SubstreamPath & substream) {
                const auto name   = DMFile::getFileNameBase(col_id, substream);
                auto &     stream = column_streams.at(name);
                stream->original_hashing.nextIfAtEnd();
            },
            {});
    }

    auto & avg_size = dmfile->column_stats.at(col_id).avg_size;
    IDataType::updateAvgValueSizeHint(column, avg_size);
}

void DMFileWriter::finalizeColumn(ColId col_id, DataTypePtr type)
{
    size_t bytes_written = 0;

    if (single_file_mode)
    {
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(col_id, substream);

            dmfile->addSubFileStat(DMFile::colDataFileName(stream_name), 0, single_file_stream->column_data_sizes.at(stream_name));

            // write mark
            size_t mark_offset_in_file = single_file_stream->plain_hashing.count();
            for (const auto & mark_with_size : single_file_stream->column_mark_with_sizes.at(stream_name))
            {
                writeIntBinary(mark_with_size.mark.offset_in_compressed_file, single_file_stream->plain_hashing);
                writeIntBinary(mark_with_size.mark.offset_in_decompressed_block, single_file_stream->plain_hashing);
                writeIntBinary(mark_with_size.mark_size, single_file_stream->plain_hashing);
            }
            size_t mark_size_in_file = single_file_stream->plain_hashing.count() - mark_offset_in_file;
            dmfile->addSubFileStat(DMFile::colMarkFileName(stream_name), mark_offset_in_file, mark_size_in_file);

            // write minmax
            auto & minmax_indexs = single_file_stream->minmax_indexs;
            if (auto iter = minmax_indexs.find(stream_name); iter != minmax_indexs.end())
            {
                size_t minmax_offset_in_file = single_file_stream->plain_hashing.count();
                iter->second->write(*type, single_file_stream->plain_hashing);
                size_t minmax_size_in_file = single_file_stream->plain_hashing.count() - minmax_offset_in_file;
                bytes_written += minmax_size_in_file;
                dmfile->addSubFileStat(DMFile::colIndexFileName(stream_name), minmax_offset_in_file, minmax_size_in_file);
            }
        };
        type->enumerateStreams(callback, {});
    }
    else
    {
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(col_id, substream);
            auto &     stream      = column_streams.at(stream_name);
            stream->flush();
            bytes_written += stream->getWrittenBytes();

            if (stream->minmaxes)
            {
                WriteBufferFromFileProvider buf(
                    file_provider, dmfile->colIndexPath(stream_name), dmfile->encryptionIndexPath(stream_name), false);
                stream->minmaxes->write(*type, buf);
                buf.sync();
                bytes_written += buf.getPositionInFile();
            }
        };
        type->enumerateStreams(callback, {});
    }

    // Update column's bytes in disk
    dmfile->column_stats.at(col_id).serialized_bytes = bytes_written;
}

} // namespace DM
} // namespace DB
