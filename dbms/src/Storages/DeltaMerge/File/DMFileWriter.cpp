// Copyright 2023 PingCAP, Inc.
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
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/S3/S3Common.h>

#ifndef NDEBUG
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif


namespace DB::DM
{

DMFileWriter::DMFileWriter(
    const DMFilePtr & dmfile_,
    const ColumnDefines & write_columns_,
    const FileProviderPtr & file_provider_,
    const WriteLimiterPtr & write_limiter_,
    const DMFileWriter::Options & options_)
    : dmfile(dmfile_)
    , write_columns(write_columns_)
    , options(options_)
    , file_provider(file_provider_)
    , write_limiter(write_limiter_)
    // Create a new file is necessarily here. Because it will create encryption info for the whole DMFile.
    , meta_file(createMetaFile())
{
    dmfile->setStatus(DMFile::Status::WRITING);

    if (dmfile->useMetaV2())
    {
        merged_file.buffer = std::make_unique<WriteBufferFromFileProvider>(
            file_provider,
            dmfile->mergedPath(0),
            dmfile->encryptionMergedPath(0),
            /*create_new_encryption_info*/ false,
            write_limiter);

        merged_file.file_info = DMFile::MergedFile({0, 0});
    }

    for (auto & cd : write_columns)
    {
        if (cd.vector_index)
            RUNTIME_CHECK(VectorIndex::isSupportedType(*cd.type));

        // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.
        /// for handle column always generate index
        auto type = removeNullable(cd.type);
        bool do_index = cd.id == EXTRA_HANDLE_COLUMN_ID || type->isInteger() || type->isDateOrDateTime();
        addStreams(cd.id, cd.type, do_index, cd.vector_index);
        dmfile->column_stats.emplace(cd.id, ColumnStat{cd.id, cd.type, /*avg_size=*/0});
    }
}

DMFileWriter::WriteBufferFromFileBasePtr DMFileWriter::createMetaFile()
{
    if (dmfile->useMetaV2())
    {
        return createMetaV2File();
    }
    else
    {
        return createPackStatsFile();
    }
}

DMFileWriter::WriteBufferFromFileBasePtr DMFileWriter::createMetaV2File()
{
    return std::make_unique<WriteBufferFromFileProvider>(
        file_provider,
        dmfile->metav2Path(),
        dmfile->encryptionMetav2Path(),
        /*create_new_encryption_info*/ true,
        write_limiter,
        DMFile::meta_buffer_size);
}

DMFileWriter::WriteBufferFromFileBasePtr DMFileWriter::createPackStatsFile()
{
    return dmfile->configuration ? createWriteBufferFromFileBaseByFileProvider(
               file_provider,
               dmfile->packStatPath(),
               dmfile->encryptionPackStatPath(),
               /*create_new_encryption_info*/ true,
               write_limiter,
               dmfile->configuration->getChecksumAlgorithm(),
               dmfile->configuration->getChecksumFrameLength())
                                 : createWriteBufferFromFileBaseByFileProvider(
                                     file_provider,
                                     dmfile->packStatPath(),
                                     dmfile->encryptionPackStatPath(),
                                     /*create_new_encryption_info*/ true,
                                     write_limiter,
                                     0,
                                     0,
                                     options.max_compress_block_size);
}

void DMFileWriter::addStreams(ColId col_id, DataTypePtr type, bool do_index, TiDB::VectorIndexInfoPtr do_vector_index)
{
    auto callback = [&](const IDataType::SubstreamPath & substream_path) {
        const auto stream_name = DMFile::getFileNameBase(col_id, substream_path);
        bool substream_can_index = !IDataType::isNullMap(substream_path) && !IDataType::isArraySizes(substream_path);
        auto stream = std::make_unique<Stream>(
            dmfile,
            stream_name,
            type,
            options.compression_settings,
            options.max_compress_block_size,
            file_provider,
            write_limiter,
            do_index && substream_can_index,
            (do_vector_index && substream_can_index) ? do_vector_index : nullptr);
        column_streams.emplace(stream_name, std::move(stream));
    };

    type->enumerateStreams(callback, {});
}


void DMFileWriter::write(const Block & block, const BlockProperty & block_property)
{
#ifndef NDEBUG
    // In the prod env, the #rows is checked in upper level
    block.checkNumberOfRows();
#endif

    is_empty_file = false;
    DMFile::PackStat stat{};
    stat.rows = block.rows();
    stat.not_clean = block_property.not_clean_rows;
    stat.bytes = block.bytes(); // This is bytes of pack data in memory.

    auto del_mark_column = tryGetByColumnId(block, TAG_COLUMN_ID).column;

    const ColumnVector<UInt8> * del_mark
        = !del_mark_column ? nullptr : static_cast<const ColumnVector<UInt8> *>(del_mark_column.get());

    for (auto & cd : write_columns)
    {
        const auto & col = getByColumnId(block, cd.id).column;
        writeColumn(cd.id, *cd.type, *col, del_mark);

        if (cd.id == VERSION_COLUMN_ID)
            stat.first_version = col->get64(0);
        else if (cd.id == TAG_COLUMN_ID)
            stat.first_tag = static_cast<UInt8>(col->get64(0));
    }

    dmfile->addPack(stat);

    auto & properties = dmfile->getPackProperties();
    auto * property = properties.add_property();
    property->set_num_rows(block_property.effective_num_rows);
    property->set_gc_hint_version(block_property.gc_hint_version);
    property->set_deleted_rows(block_property.deleted_rows);
}

void DMFileWriter::finalize()
{
    for (auto & cd : write_columns)
    {
        finalizeColumn(cd.id, cd.type);
    }
    if (dmfile->useMetaV2())
    {
        dmfile->finalizeSmallFiles(merged_file, file_provider, write_limiter);
        // Some fields of ColumnStat is set in `finalizeColumn`, must call finalizeMetaV2 after all column finalized
        finalizeMetaV2();
    }
    else
    {
        finalizeMetaV1();
    }
}

void DMFileWriter::finalizeMetaV1()
{
    const auto & pack_stats = dmfile->getPackStats();
    for (const auto & pack_stat : pack_stats)
    {
        writePODBinary(pack_stat, *meta_file);
    }
    meta_file->sync();
    meta_file.reset();
    dmfile->finalizeForFolderMode(file_provider, write_limiter);
}

void DMFileWriter::finalizeMetaV2()
{
    dmfile->finalizeMetaV2(*meta_file);
    meta_file->sync();
    meta_file.reset();
    dmfile->finalizeDirName();
}

void DMFileWriter::writeColumn(
    ColId col_id,
    const IDataType & type,
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark)
{
    size_t rows = column.size();
    type.enumerateStreams(
        [&](const IDataType::SubstreamPath & substream) {
            const auto name = DMFile::getFileNameBase(col_id, substream);
            auto & stream = column_streams.at(name);
            if (stream->minmaxes)
            {
                // For EXTRA_HANDLE_COLUMN_ID, we ignore del_mark when add minmax index.
                // Because we need all rows which satisfy a certain range when place delta index no matter whether the row is a delete row.
                // For TAG Column, we also ignore del_mark when add minmax index.
                stream->minmaxes->addPack(
                    column,
                    (col_id == EXTRA_HANDLE_COLUMN_ID || col_id == TAG_COLUMN_ID) ? nullptr : del_mark);
            }

            if (stream->vector_index)
                stream->vector_index->addBlock(column, del_mark);

            /// There could already be enough data to compress into the new block.
            if (stream->compressed_buf->offset() >= options.min_compress_block_size)
                stream->compressed_buf->next();

            auto offset_in_compressed_block = stream->compressed_buf->offset();

            if (dmfile->useMetaV2())
            {
                stream->marks->emplace_back(
                    MarkInCompressedFile{stream->plain_file->count(), offset_in_compressed_block});
            }
            else
            {
                writeIntBinary(stream->plain_file->count(), *stream->mark_file);
                writeIntBinary(offset_in_compressed_block, *stream->mark_file);
            }
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

    auto & avg_size = dmfile->column_stats.at(col_id).avg_size;
    IDataType::updateAvgValueSizeHint(column, avg_size);
}

void DMFileWriter::finalizeColumn(ColId col_id, DataTypePtr type)
{
    // Update column's bytes in memory
    auto & col_stat = dmfile->column_stats.at(col_id);

#ifndef NDEBUG
    auto examine_buffer_size = [&](auto & buf, auto & fp) {
        if (!fp.isEncryptionEnabled())
        {
            auto fd = buf.getFD();
            struct stat file_stat = {};
            ::fstat(fd, &file_stat);
            assert(buf.getMaterializedBytes() == file_stat.st_size);
        }
    };
#endif
    auto callback = [&](const IDataType::SubstreamPath & substream) {
        const auto stream_name = DMFile::getFileNameBase(col_id, substream);
        auto & stream = column_streams.at(stream_name);

        const bool is_null = IDataType::isNullMap(substream);
        const bool is_array = IDataType::isArraySizes(substream);

        // v3
        if (dmfile->useMetaV2())
        {
            stream->compressed_buf->next();
            stream->plain_file->next();
            stream->plain_file->sync();

            col_stat.serialized_bytes += stream->plain_file->getMaterializedBytes();

            if (is_null)
            {
                col_stat.nullmap_data_bytes = stream->plain_file->getMaterializedBytes();
            }
            else if (is_array)
            {
                col_stat.array_sizes_bytes = stream->plain_file->getMaterializedBytes();
            }
            else
            {
                col_stat.data_bytes = stream->plain_file->getMaterializedBytes();
            }

#ifndef NDEBUG
            examine_buffer_size(*stream->plain_file, *this->file_provider);
#endif

            // write index info into merged_file_writer
            if (stream->minmaxes and !is_empty_file)
            {
                dmfile->checkMergedFile(merged_file, file_provider, write_limiter);

                auto fname = dmfile->colIndexFileName(stream_name);

                auto buffer = createWriteBufferFromFileBaseByWriterBuffer(
                    merged_file.buffer,
                    dmfile->configuration->getChecksumAlgorithm(),
                    dmfile->configuration->getChecksumFrameLength());

                stream->minmaxes->write(*type, *buffer);

                col_stat.index_bytes = buffer->getMaterializedBytes();

                MergedSubFileInfo info{
                    fname,
                    merged_file.file_info.number,
                    merged_file.file_info.size,
                    col_stat.index_bytes};
                dmfile->merged_sub_file_infos[fname] = info;

                merged_file.file_info.size += col_stat.index_bytes;
                buffer->next();
            }

            if (stream->vector_index && !is_empty_file)
            {
                dmfile->checkMergedFile(merged_file, file_provider, write_limiter);

                auto fname = dmfile->colIndexFileName(stream_name);

                auto buffer = createWriteBufferFromFileBaseByWriterBuffer(
                    merged_file.buffer,
                    dmfile->configuration->getChecksumAlgorithm(),
                    dmfile->configuration->getChecksumFrameLength());

                stream->vector_index->serializeBinary(*buffer);

                col_stat.index_bytes = buffer->getMaterializedBytes();

                // Memorize what kind of vector index it is, so that we can correctly restore it when reading.
                col_stat.vector_index = dtpb::ColumnVectorIndexInfo{};
                col_stat.vector_index->set_index_kind(String(magic_enum::enum_name(stream->vector_index->kind)));
                col_stat.vector_index->set_distance_metric(
                    String(magic_enum::enum_name(stream->vector_index->distance_metric)));

                MergedSubFileInfo info{
                    fname,
                    merged_file.file_info.number,
                    merged_file.file_info.size,
                    col_stat.index_bytes};
                dmfile->merged_sub_file_infos[fname] = info;

                merged_file.file_info.size += col_stat.index_bytes;
                buffer->next();
            }

            // write mark into merged_file_writer
            if (!is_empty_file)
            {
                dmfile->checkMergedFile(merged_file, file_provider, write_limiter);

                auto fname = dmfile->colMarkFileName(stream_name);

                auto buffer = createWriteBufferFromFileBaseByWriterBuffer(
                    merged_file.buffer,
                    dmfile->configuration->getChecksumAlgorithm(),
                    dmfile->configuration->getChecksumFrameLength());


                for (const auto & mark : *(stream->marks))
                {
                    writeIntBinary(mark.offset_in_compressed_file, *buffer);
                    writeIntBinary(mark.offset_in_decompressed_block, *buffer);
                }
                size_t mark_size = buffer->getMaterializedBytes();
                MergedSubFileInfo info{fname, merged_file.file_info.number, merged_file.file_info.size, mark_size};
                dmfile->merged_sub_file_infos[fname] = info;

                merged_file.file_info.size += mark_size;
                buffer->next();

                if (is_null)
                {
                    col_stat.nullmap_mark_bytes = mark_size;
                }
                else if (is_array)
                {
                    col_stat.array_sizes_mark_bytes = mark_size;
                }
                else
                {
                    col_stat.mark_bytes = mark_size;
                }
            }
        }
        else
        {
            // v1 or v2
            stream->compressed_buf->next();
            stream->plain_file->next();
            stream->plain_file->sync();
            stream->mark_file->sync();
#ifndef NDEBUG
            examine_buffer_size(*stream->mark_file, *this->file_provider);
            examine_buffer_size(*stream->plain_file, *this->file_provider);
#endif
            col_stat.serialized_bytes
                += stream->plain_file->getMaterializedBytes() + stream->mark_file->getMaterializedBytes();
            if (is_null)
            {
                col_stat.nullmap_data_bytes = stream->plain_file->getMaterializedBytes();
                col_stat.nullmap_mark_bytes = stream->mark_file->getMaterializedBytes();
            }
            else
            {
                col_stat.data_bytes = stream->plain_file->getMaterializedBytes();
                col_stat.mark_bytes = stream->mark_file->getMaterializedBytes();
            }


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
                    col_stat.index_bytes = buf.getMaterializedBytes();
                    col_stat.serialized_bytes += is_empty_file ? 0 : col_stat.index_bytes;
                }
                else
                {
                    auto buf = createWriteBufferFromFileBaseByFileProvider(
                        file_provider,
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
                    col_stat.index_bytes = buf->getMaterializedBytes();
                    col_stat.serialized_bytes += is_empty_file ? 0 : col_stat.index_bytes;

#ifndef NDEBUG
                    examine_buffer_size(*buf, *this->file_provider);
#endif
                }
            }

            if (stream->vector_index)
            {
                RUNTIME_CHECK_MSG(false, "Vector index is not compatible with V1 and V2 format");
            }
        }
    };
    type->enumerateStreams(callback, {});
}

} // namespace DB::DM
