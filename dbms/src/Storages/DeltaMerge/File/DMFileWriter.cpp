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
      plain_file(createWriteBufferFromFileBaseByFileProvider(file_provider,
                                                       dmfile->path(),
                                                       EncryptionPath(dmfile->path(), ""),
                                                       true,
                                                       0,
                                                       0,
                                                       max_compress_block_size)),
      compressed_buf(*plain_file, compression_settings)
{
    dmfile->setStatus(DMFile::Status::WRITING);
    for (auto & cd : write_columns)
    {
        // TODO: currently we only generate index for Integers, Date, DateTime types, and this should be configurable by user.
        // TODO: If column type is nullable, we won't generate index for it
        if (cd.type->isInteger() || cd.type->isDateOrDateTime())
        {
            String column_name = DMFile::getFileNameBase(cd.id, {});
            minmaxindexs.emplace(column_name, std::make_shared<MinMaxIndex>(*cd.type));
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
        finalizeColumn(cd.id, *(cd.type));
    }

    dmfile->finalize(*plain_file);
}

void DMFileWriter::writeColumn(ColId col_id, const IDataType & type)
{
    MarksInCompressedFile marks{blocks.size()};
    size_t column_offset_in_file = plain_file->count();
    MinMaxIndexPtr minmax = nullptr;
    size_t bytes_written = 0;
    if (auto iter = minmaxindexs.find(DMFile::getFileNameBase(col_id, {})); iter != minmaxindexs.end())
    {
        minmax = iter->second;
    }
    for (size_t i = 0; i < blocks.size(); i++)
    {
        auto & block = blocks[i];
        auto & column = getByColumnId(block, col_id).column;
        size_t rows = column->size();

        if (compressed_buf.offset() >= min_compress_block_size)
        {
            compressed_buf.next();
        }
        auto offset_in_compressed_block = compressed_buf.offset();
        marks[i] = MarkInCompressedFile{
            // TODO: make sure this two attribute are correct
            .offset_in_compressed_file = plain_file->count(),
            .offset_in_decompressed_block = offset_in_compressed_block
        };
        if (type.isNullable())
        {
            const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
            nullable_type.getNestedType()->serializeBinaryBulk(*column, compressed_buf, 0, rows);
        }
        else
        {
            type.serializeBinaryBulk(*column, compressed_buf, 0, rows);
        }
        if (minmax)
        {
            minmax->addPack(*column, nullptr);
        }
        auto & avg_size = dmfile->column_stats.at(col_id).avg_size;
        IDataType::updateAvgValueSizeHint(*column, avg_size);
    }
    compressed_buf.next();
    size_t column_size_in_file = plain_file->count() - column_offset_in_file;
    dmfile->addSubFileStat(DMFile::colDataIdentifier(DMFile::getFileNameBase(col_id, {})), column_offset_in_file, column_size_in_file);
    bytes_written += column_size_in_file;

    size_t mark_offset_in_file = plain_file->count();
    for (auto mark : marks)
    {
        writeIntBinary(mark.offset_in_compressed_file, *plain_file);
        writeIntBinary(mark.offset_in_decompressed_block, *plain_file);
    }
    size_t mark_size_in_file = plain_file->count() - mark_offset_in_file;
    dmfile->addSubFileStat(DMFile::colMarkIdentifier(DMFile::getFileNameBase(col_id, {})), mark_offset_in_file, mark_size_in_file);

    if (minmax)
    {
        size_t minmax_offset_in_file = plain_file->count();
        minmax->write(type, *plain_file);
        size_t minmax_size_in_file = plain_file->count() - minmax_offset_in_file;
        bytes_written += minmax_size_in_file;
        dmfile->addSubFileStat(DMFile::colIndexIdentifier(DMFile::getFileNameBase(col_id, {})), minmax_offset_in_file, minmax_size_in_file);
    }

    if (type.isNullable())
    {
        MarksInCompressedFile nullmap_marks{blocks.size()};
        size_t nullmap_offset_in_file = plain_file->count();
        for (size_t i = 0; i < blocks.size(); i++)
        {
            auto & block = blocks[i];
            auto & column = getByColumnId(block, col_id).column;
            size_t rows = column->size();

            if (compressed_buf.offset() >= min_compress_block_size)
            {
                compressed_buf.next();
            }
            auto offset_in_compressed_block = compressed_buf.offset();
            nullmap_marks[i] = MarkInCompressedFile{
                // TODO: make sure this two attribute are correct
                .offset_in_compressed_file = plain_file->count(),
                .offset_in_decompressed_block = offset_in_compressed_block
            };
            const ColumnNullable & col = static_cast<const ColumnNullable &>(*column);
            col.checkConsistency();
            DataTypeUInt8().serializeBinaryBulk(col.getNullMapColumn(), compressed_buf, 0, rows);
        }
        compressed_buf.next();
        size_t nullmap_size_in_file = plain_file->count() - nullmap_offset_in_file;
        dmfile->addSubFileStat(DMFile::colDataIdentifier(DMFile::getFileNameBase(col_id, {IDataType::Substream::NullMap})), nullmap_offset_in_file, nullmap_size_in_file);

        size_t nullmap_mark_offset_in_file = plain_file->count();
        for (auto mark : nullmap_marks)
        {
            writeIntBinary(mark.offset_in_compressed_file, *plain_file);
            writeIntBinary(mark.offset_in_decompressed_block, *plain_file);
        }
        size_t nullmap_mark_size_in_file = plain_file->count() - nullmap_mark_offset_in_file;
        dmfile->addSubFileStat(DMFile::colMarkIdentifier(DMFile::getFileNameBase(col_id, {IDataType::Substream::NullMap})), nullmap_mark_size_in_file, nullmap_mark_offset_in_file);
    }

    // Update column's bytes in disk
    dmfile->column_stats.at(col_id).serialized_bytes = bytes_written;
}

void DMFileWriter::finalizeColumn(ColId col_id, const IDataType & type)
{
    writeColumn(col_id, type);
}

} // namespace DM
} // namespace DB
