// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/TiFlashException.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>

#ifndef NDEBUG
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif


namespace DB
{
namespace DM
{
DMFileWriter::DMFileWriter(const DMFilePtr & dmfile_,
                           const ColumnDefines & write_columns_,
                           const FileProviderPtr & file_provider_,
                           const WriteLimiterPtr & write_limiter_,
                           const DMFileWriter::Options & options_)
    : dmfile(dmfile_)
    , write_columns(write_columns_)
    , options(options_, dmfile)
    ,
    // assume pack_stat_file is the first file created inside DMFile
    // it will create encryption info for the whole DMFile
    pack_stat_file(
        (options.flags.isSingleFile()) //
            ? nullptr
            : (dmfile->configuration ? createWriteBufferFromFileBaseByFileProvider(
                   file_provider_,
                   dmfile->packStatPath(),
                   dmfile->encryptionPackStatPath(),
                   true,
                   write_limiter_,
                   dmfile->configuration->getChecksumAlgorithm(),
                   dmfile->configuration->getChecksumFrameLength())
                                     : createWriteBufferFromFileBaseByFileProvider(file_provider_,
                                                                                   dmfile->packStatPath(),
                                                                                   dmfile->encryptionPackStatPath(),
                                                                                   true,
                                                                                   write_limiter_,
                                                                                   0,
                                                                                   0,
                                                                                   options.max_compress_block_size)))
    , single_file_stream((!options.flags.isSingleFile())
                             ? nullptr
                             : new SingleFileStream(
                                 dmfile_,
                                 options.compression_settings,
                                 options.max_compress_block_size,
                                 file_provider_,
                                 write_limiter_))
    , file_provider(file_provider_)
    , write_limiter(write_limiter_)
{
    dmfile->setStatus(DMFile::Status::WRITING);
    for (auto & cd : write_columns)
    {
        // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.
        // TODO: If column type is nullable, we won't generate index for it
        /// for handle column always generate index
        bool do_index = cd.id == EXTRA_HANDLE_COLUMN_ID || cd.type->isInteger() || cd.type->isDateOrDateTime();

        if (options.flags.isSingleFile())
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
        auto stream = std::make_unique<Stream>(
            dmfile,
            stream_name,
            type,
            options.compression_settings,
            options.max_compress_block_size,
            file_provider,
            write_limiter,
            IDataType::isNullMap(substream_path) ? false : do_index);
        column_streams.emplace(stream_name, std::move(stream));
    };

    type->enumerateStreams(callback, {});
}


void DMFileWriter::write(const Block & block, const BlockProperty & block_property)
{
    is_empty_file = false;
    DMFile::PackStat stat;
    stat.rows = block.rows();
    stat.not_clean = block_property.not_clean_rows;
    stat.bytes = block.bytes(); // This is bytes of pack data in memory.

    auto del_mark_column = tryGetByColumnId(block, TAG_COLUMN_ID).column;

    const ColumnVector<UInt8> * del_mark = !del_mark_column ? nullptr : static_cast<const ColumnVector<UInt8> *>(del_mark_column.get());

    for (auto & cd : write_columns)
    {
        const auto & col = getByColumnId(block, cd.id).column;
        writeColumn(cd.id, *cd.type, *col, del_mark);

        if (cd.id == VERSION_COLUMN_ID)
            stat.first_version = col->get64(0);
        else if (cd.id == TAG_COLUMN_ID)
            stat.first_tag = static_cast<UInt8>(col->get64(0));
    }

    if (!options.flags.isSingleFile())
    {
        writePODBinary(stat, *pack_stat_file);
    }

    dmfile->addPack(stat);

    auto & properties = dmfile->getPackProperties();
    auto * property = properties.add_property();
    property->set_num_rows(block_property.effective_num_rows);
    property->set_gc_hint_version(block_property.gc_hint_version);
}

void DMFileWriter::finalize()
{
    if (!options.flags.isSingleFile())
    {
        pack_stat_file->sync();
    }

    for (auto & cd : write_columns)
    {
        finalizeColumn(cd.id, cd.type);
    }

    if (options.flags.isSingleFile())
    {
        dmfile->finalizeForSingleFileMode(single_file_stream->plain_layer);
        single_file_stream->flush();
    }
    else
    {
        dmfile->finalizeForFolderMode(file_provider, write_limiter);
    }
}

void DMFileWriter::writeColumn(ColId col_id, const IDataType & type, const IColumn & column, const ColumnVector<UInt8> * del_mark)
{
    size_t rows = column.size();

    if (options.flags.isSingleFile())
    {
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            size_t offset_in_compressed_file = single_file_stream->plain_layer.count();
            const auto stream_name = DMFile::getFileNameBase(col_id, substream);
            if (unlikely(substream.size() > 1))
                throw DB::TiFlashException("Substream_path shouldn't be more than one.", Errors::DeltaTree::Internal);

            auto & minmax_indexs = single_file_stream->minmax_indexs;
            if (auto iter = minmax_indexs.find(stream_name); iter != minmax_indexs.end())
            {
                // For EXTRA_HANDLE_COLUMN_ID, we ignore del_mark when add minmax index.
                // Because we need all rows which satisfy a certain range when place delta index no matter whether the row is a delete row.
                iter->second->addPack(column, col_id == EXTRA_HANDLE_COLUMN_ID ? nullptr : del_mark);
            }

            auto offset_in_compressed_block = single_file_stream->original_layer.offset();
            if (unlikely(offset_in_compressed_block != 0))
                throw DB::TiFlashException("Offset in compressed block is always expected to be 0 when single_file_mode is true, now "
                                               + DB::toString(offset_in_compressed_block),
                                           Errors::DeltaTree::Internal);

            // write column data
            if (substream.empty())
            {
                if (unlikely(type.isNullable()))
                    throw DB::TiFlashException("Type shouldn't be nullable when substream_path is empty.", Errors::DeltaTree::Internal);

                type.serializeBinaryBulk(column, single_file_stream->original_layer, 0, rows);
            }
            else if (substream[0].type == IDataType::Substream::NullMap)
            {
                if (unlikely(!type.isNullable()))
                    throw DB::TiFlashException(
                        "Type shouldn be nullable when substream_path's type is NullMap.",
                        Errors::DeltaTree::Internal);

                const ColumnNullable & col = static_cast<const ColumnNullable &>(column);
                col.checkConsistency();
                DataTypeUInt8().serializeBinaryBulk(col.getNullMapColumn(), single_file_stream->original_layer, 0, rows);
            }
            else if (substream[0].type == IDataType::Substream::NullableElements)
            {
                if (unlikely(!type.isNullable()))
                    throw DB::TiFlashException(
                        "Type shouldn be nullable when substream_path's type is NullableElements.",
                        Errors::DeltaTree::Internal);

                const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
                const ColumnNullable & col = static_cast<const ColumnNullable &>(column);
                nullable_type.getNestedType()->serializeBinaryBulk(col.getNestedColumn(), single_file_stream->original_layer, 0, rows);
            }
            else
            {
                throw DB::TiFlashException(
                    "Unknown type of substream_path: " + std::to_string(substream[0].type),
                    Errors::DeltaTree::Internal);
            }
            single_file_stream->flushCompressedData();
            size_t mark_size_in_file = single_file_stream->plain_layer.count() - offset_in_compressed_file;
            single_file_stream->column_mark_with_sizes.at(stream_name)
                .push_back(MarkWithSizeInCompressedFile{MarkInCompressedFile{.offset_in_compressed_file = offset_in_compressed_file,
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
                const auto name = DMFile::getFileNameBase(col_id, substream);
                auto & stream = column_streams.at(name);
                if (stream->minmaxes)
                {
                    // For EXTRA_HANDLE_COLUMN_ID, we ignore del_mark when add minmax index.
                    // Because we need all rows which satisfy a certain range when place delta index no matter whether the row is a delete row.
                    stream->minmaxes->addPack(column, col_id == EXTRA_HANDLE_COLUMN_ID ? nullptr : del_mark);
                }

                /// There could already be enough data to compress into the new block.
                if (stream->compressed_buf->offset() >= options.min_compress_block_size)
                    stream->compressed_buf->next();

                auto offset_in_compressed_block = stream->compressed_buf->offset();

                writeIntBinary(stream->plain_file->count(), *stream->mark_file);
                writeIntBinary(offset_in_compressed_block, *stream->mark_file);
            },
            {});

        type.serializeBinaryBulkWithMultipleStreams(
            column,
            [&](const IDataType::SubstreamPath & substream) {
                const auto stream_name = DMFile::getFileNameBase(col_id, substream);
                auto & stream = column_streams.at(stream_name);
                return &(*stream->compressed_buf);
            },
            0,
            rows,
            true,
            {});

        type.enumerateStreams(
            [&](const IDataType::SubstreamPath & substream) {
                const auto name = DMFile::getFileNameBase(col_id, substream);
                auto & stream = column_streams.at(name);
                stream->compressed_buf->nextIfAtEnd();
            },
            {});
    }

    auto & avg_size = dmfile->column_stats.at(col_id).avg_size;
    IDataType::updateAvgValueSizeHint(column, avg_size);
}

void DMFileWriter::finalizeColumn(ColId col_id, DataTypePtr type)
{
    size_t bytes_written = 0;
#ifndef NDEBUG
    auto examine_buffer_size = [](auto & buf, auto & fp) {
        if (!fp.isEncryptionEnabled())
        {
            auto fd = buf.getFD();
            struct stat file_stat = {};
            ::fstat(fd, &file_stat);
            assert(buf.getMaterializedBytes() == file_stat.st_size);
        }
    };
#endif
    if (options.flags.isSingleFile())
    {
        auto callback = [&](const IDataType::SubstreamPath & substream) {
            const auto stream_name = DMFile::getFileNameBase(col_id, substream);

            dmfile->addSubFileStat(DMFile::colDataFileName(stream_name), 0, single_file_stream->column_data_sizes.at(stream_name));

            // write mark
            size_t mark_offset_in_file = single_file_stream->plain_layer.count();
            for (const auto & mark_with_size : single_file_stream->column_mark_with_sizes.at(stream_name))
            {
                writeIntBinary(mark_with_size.mark.offset_in_compressed_file, single_file_stream->plain_layer);
                writeIntBinary(mark_with_size.mark.offset_in_decompressed_block, single_file_stream->plain_layer);
                writeIntBinary(mark_with_size.mark_size, single_file_stream->plain_layer);
            }
            size_t mark_size_in_file = single_file_stream->plain_layer.count() - mark_offset_in_file;
            dmfile->addSubFileStat(DMFile::colMarkFileName(stream_name), mark_offset_in_file, mark_size_in_file);

            // write minmax
            auto & minmax_indexs = single_file_stream->minmax_indexs;
            if (auto iter = minmax_indexs.find(stream_name); iter != minmax_indexs.end())
            {
                size_t minmax_offset_in_file = single_file_stream->plain_layer.count();
                iter->second->write(*type, single_file_stream->plain_layer);
                size_t minmax_size_in_file = single_file_stream->plain_layer.count() - minmax_offset_in_file;
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
            auto & stream = column_streams.at(stream_name);
            stream->flush();
#ifndef NDEBUG
            examine_buffer_size(*stream->mark_file, *this->file_provider);
            examine_buffer_size(*stream->plain_file, *this->file_provider);
#endif
            bytes_written += stream->getWrittenBytes();

            if (stream->minmaxes)
            {
                if (!dmfile->configuration)
                {
                    WriteBufferFromFileProvider buf(
                        file_provider,
                        dmfile->colIndexPath(stream_name),
                        dmfile->encryptionIndexPath(stream_name),
                        false,
                        write_limiter);
                    stream->minmaxes->write(*type, buf);
                    buf.sync();
                    // Ignore data written in index file when the dmfile is empty.
                    // This is ok because the index file in this case is tiny, and we already ignore other small files like meta and pack stat file.
                    // The motivation to do this is to show a zero `stable_size_on_disk` for empty segments,
                    // and we cannot change the index file format for empty dmfile because of backward compatibility.
                    bytes_written += is_empty_file ? 0 : buf.getMaterializedBytes();
                }
                else
                {
                    auto buf = createWriteBufferFromFileBaseByFileProvider(file_provider,
                                                                           dmfile->colIndexPath(stream_name),
                                                                           dmfile->encryptionIndexPath(stream_name),
                                                                           false,
                                                                           write_limiter,
                                                                           dmfile->configuration->getChecksumAlgorithm(),
                                                                           dmfile->configuration->getChecksumFrameLength());
                    stream->minmaxes->write(*type, *buf);
                    buf->sync();
                    // Ignore data written in index file when the dmfile is empty.
                    // This is ok because the index file in this case is tiny, and we already ignore other small files like meta and pack stat file.
                    // The motivation to do this is to show a zero `stable_size_on_disk` for empty segments,
                    // and we cannot change the index file format for empty dmfile because of backward compatibility.
                    bytes_written += is_empty_file ? 0 : buf->getMaterializedBytes();
#ifndef NDEBUG
                    examine_buffer_size(*buf, *this->file_provider);
#endif
                }
            }
        };
        type->enumerateStreams(callback, {});
    }

    // Update column's bytes in disk
    dmfile->column_stats.at(col_id).serialized_bytes = bytes_written;
}

} // namespace DM
} // namespace DB
