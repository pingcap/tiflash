#include <Storages/DeltaMerge/ColumnFile/ColumnBigFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnDeleteRangeFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnTinyFile.h>

namespace DB
{
namespace DM
{
void serializeColumnStableFiles_V3(WriteBuffer & buf, const ColumnFiles & column_files)
{
    size_t saved_packs = std::find_if(column_files.begin(), column_files.end(), [](const ColumnFilePtr & p) { return !p->isSaved(); }) - column_files.begin();

    writeIntBinary(saved_packs, buf);
    BlockPtr last_schema;

    for (const auto & column_file : column_files)
    {
        if (!column_file->isSaved())
            break;
        // Do not encode the schema if it is the same as previous one.
        writeIntBinary(column_file->getType(), buf);

        switch (column_file->getType())
        {
        case ColumnFile::Type::DELETE_RANGE:
        {
            column_file->serializeMetadata(buf, false);
            break;
        }
        case ColumnFile::Type::BIG_FILE:
        {
            column_file->serializeMetadata(buf, false);
            break;
        }
        case ColumnFile::Type::TINY_FILE:
        {
            auto * tiny_file = column_file->tryToTinyFile();
            auto cur_schema = tiny_file->getSchema();
            if (unlikely(!cur_schema))
                throw Exception("A tiny file without schema: " + column_file->toString(), ErrorCodes::LOGICAL_ERROR);

            bool save_schema = cur_schema != last_schema;
            column_file->serializeMetadata(buf, save_schema);
            break;
        }
        default:
            throw Exception("Unexpected type", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

ColumnFiles deserializeColumnStableFiles_V3(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf, UInt64 /*version*/)
{
    size_t column_file_count;
    readIntBinary(column_file_count, buf);
    ColumnFiles column_files;
    BlockPtr last_schema;
    for (size_t i = 0; i < column_file_count; ++i)
    {
        std::underlying_type<ColumnFile::Type>::type column_file_type;
        readIntBinary(column_file_type, buf);
        ColumnFilePtr column_file;
        switch (column_file_type)
        {
        case ColumnFile::Type::DELETE_RANGE:
            column_file = ColumnDeleteRangeFile::deserializeMetadata(buf);
            break;
        case ColumnFile::Type::TINY_FILE:
        {
            std::tie(column_file, last_schema) = ColumnTinyFile::deserializeMetadata(buf, last_schema);
            break;
        }
        case ColumnFile::Type::BIG_FILE:
        {
            column_file = ColumnBigFile::deserializeMetadata(context, segment_range, buf);
            break;
        }
        default:
            throw Exception("Unexpected column file type: " + DB::toString(column_file_type), ErrorCodes::LOGICAL_ERROR);
        }
        column_files.emplace_back(std::move(column_file));
    }
    return column_files;
}

} // namespace DM
} // namespace DB
