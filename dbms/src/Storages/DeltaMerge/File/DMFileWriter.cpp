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
                           const FileProviderPtr &     file_provider_)
    : dmfile(dmfile_),
      write_columns(write_columns_),
      min_compress_block_size(min_compress_block_size_),
      max_compress_block_size(max_compress_block_size_),
      compression_settings(compression_settings_),
      file_provider(file_provider_),
      plain_file(createWriteBufferFromFileBaseByFileProvider(
          file_provider, dmfile->path(), EncryptionPath(dmfile->encryptionBasePath(), ""), true, 0, 0, max_compress_block_size)),
      compressed_buf(*plain_file, compression_settings)
{
    dmfile->setStatus(DMFile::Status::WRITING);
    for (auto & cd : write_columns)
    {
        // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.
        // TODO: If column type is nullable, we won't generate index for it
        /// for handle column always generate index
        bool do_index = cd.id == EXTRA_HANDLE_COLUMN_ID || cd.type->isInteger() || cd.type->isDateOrDateTime();
        if (do_index)
        {
            String column_name = DMFile::getFileNameBase(cd.id, {});
            minmax_indexs.emplace(column_name, std::make_shared<MinMaxIndex>(*cd.type));
        }
        dmfile->column_stats.emplace(cd.id, ColumnStat{cd.id, cd.type, /*avg_size=*/0});
    }
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

        if (cd.id == VERSION_COLUMN_ID)
            stat.first_version = col->get64(0);
        else if (cd.id == TAG_COLUMN_ID)
            stat.first_tag = (UInt8)(col->get64(0));
    }

    dmfile->addPack(stat);
    blocks.emplace_back(block);
}

void DMFileWriter::finalize()
{
    for (auto & cd : write_columns)
    {
        finalizeColumn(cd.id, cd.type);
    }

    dmfile->finalize(*plain_file);
}

void DMFileWriter::finalizeColumn(ColId col_id, DataTypePtr type)
{
    size_t bytes_written = 0;
    auto   callback      = [&](const IDataType::SubstreamPath & substream_path) {
        String stream_name = DMFile::getFileNameBase(col_id, substream_path);
        if (unlikely(substream_path.size() > 1))
        {
            throw DB::TiFlashException("Substream_path shouldn't be more than one.", Errors::DeltaTree::Internal);
        }
        MarksInCompressedFile marks{blocks.size()};
        size_t                column_offset_in_file = plain_file->count();
        MinMaxIndexPtr        minmax_index          = nullptr;
        if (auto iter = minmax_indexs.find(DMFile::getFileNameBase(col_id)); iter != minmax_indexs.end())
        {
            minmax_index = iter->second;
        }

        // write column data
        for (size_t i = 0; i < blocks.size(); i++)
        {
            auto & block  = blocks[i];
            auto & column = getByColumnId(block, col_id).column;
            size_t rows   = column->size();
            if (compressed_buf.offset() >= min_compress_block_size)
            {
                compressed_buf.next();
            }
            auto offset_in_compressed_block = compressed_buf.offset();

            marks[i] = MarkInCompressedFile{.offset_in_compressed_file    = plain_file->count(),
                                            .offset_in_decompressed_block = offset_in_compressed_block};

            if (substream_path.empty())
            {
                if (unlikely(type->isNullable()))
                {
                    throw DB::TiFlashException("Type shouldn't be nullable when substream_path is empty.", Errors::DeltaTree::Internal);
                }
                type->serializeBinaryBulk(*column, compressed_buf, 0, rows);
            }
            else if (substream_path[0].type == IDataType::Substream::NullMap)
            {
                if (unlikely(!type->isNullable()))
                {
                    throw DB::TiFlashException("Type shouldn be nullable when substream_path's type is NullMap.",
                                               Errors::DeltaTree::Internal);
                }
                const ColumnNullable & col = static_cast<const ColumnNullable &>(*column);
                col.checkConsistency();
                DataTypeUInt8().serializeBinaryBulk(col.getNullMapColumn(), compressed_buf, 0, rows);
            }
            else if (substream_path[0].type == IDataType::Substream::NullableElements)
            {
                if (unlikely(!type->isNullable()))
                {
                    throw DB::TiFlashException("Type shouldn be nullable when substream_path's type is NullableElements.",
                                               Errors::DeltaTree::Internal);
                }
                const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*type);
                const ColumnNullable &   col           = static_cast<const ColumnNullable &>(*column);
                nullable_type.getNestedType()->serializeBinaryBulk(col.getNestedColumn(), compressed_buf, 0, rows);
            }
            else
            {
                throw DB::TiFlashException("Unknown type of substream_path: " + std::to_string(substream_path[0].type),
                                           Errors::DeltaTree::Internal);
            }

            if (minmax_index)
            {
                minmax_index->addPack(*column, nullptr);
            }
            auto & avg_size = dmfile->column_stats.at(col_id).avg_size;
            IDataType::updateAvgValueSizeHint(*column, avg_size);
        }
        compressed_buf.next();
        size_t column_size_in_file = plain_file->count() - column_offset_in_file;
        dmfile->addSubFileStat(DMFile::colDataIdentifier(stream_name), column_offset_in_file, column_size_in_file);
        bytes_written += column_size_in_file;

        // write mark data
        size_t mark_offset_in_file = plain_file->count();
        for (auto mark : marks)
        {
            writeIntBinary(mark.offset_in_compressed_file, *plain_file);
            writeIntBinary(mark.offset_in_decompressed_block, *plain_file);
        }
        size_t mark_size_in_file = plain_file->count() - mark_offset_in_file;
        dmfile->addSubFileStat(DMFile::colMarkIdentifier(stream_name), mark_offset_in_file, mark_size_in_file);

        // write minmax_index data if any
        if (minmax_index)
        {
            size_t minmax_offset_in_file = plain_file->count();
            minmax_index->write(*type, *plain_file);
            size_t minmax_size_in_file = plain_file->count() - minmax_offset_in_file;
            bytes_written += minmax_size_in_file;
            dmfile->addSubFileStat(DMFile::colIndexIdentifier(stream_name), minmax_offset_in_file, minmax_size_in_file);
        }
    };

    type->enumerateStreams(callback, {});

    // Update column's bytes in disk
    dmfile->column_stats.at(col_id).serialized_bytes = bytes_written;
}

} // namespace DM
} // namespace DB
