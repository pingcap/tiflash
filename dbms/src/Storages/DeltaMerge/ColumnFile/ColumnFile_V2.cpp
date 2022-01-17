#include <Storages/DeltaMerge/ColumnFile/ColumnBigFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnDeleteRangeFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnStableFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnTinyFile.h>

namespace DB
{
namespace DM
{
struct ColumnStableFile_V2
{
    UInt64 rows = 0;
    UInt64 bytes = 0;
    BlockPtr schema;
    RowKeyRange delete_range;
    PageId data_page_id = 0;

    bool isDeleteRange() const { return !delete_range.none(); }
};
using ColumnStableFile_V2Ptr = std::shared_ptr<ColumnStableFile_V2>;
using ColumnStableFiles_V2 = std::vector<ColumnStableFile_V2Ptr>;

inline ColumnStableFiles transform_V2_to_V3(const ColumnStableFiles_V2 & column_files_v2)
{
    ColumnStableFiles column_files_v3;
    for (auto & f : column_files_v2)
    {
        ColumnStableFilePtr f_v3 = f->isDeleteRange() ? std::make_shared<ColumnDeleteRangeFile>(std::move(f->delete_range))
                                                : std::make_shared<ColumnTinyFile>(f->schema, f->rows, f->bytes, f->data_page_id);
        column_files_v3.push_back(f_v3);
    }
    return column_files_v3;
}

inline ColumnStableFiles_V2 transformSaved_V3_to_V2(const ColumnStableFiles & column_files_v3)
{
    ColumnStableFiles_V2 column_files_v2;
    for (auto & f : column_files_v3)
    {
        auto f_v2 = new ColumnStableFile_V2();

        if (auto f_delete = f->tryToDeleteRange(); f_delete)
        {
            f_v2->delete_range = f_delete->getDeleteRange();
        }
        else if (auto f_tiny_file = f->tryToTinyFile(); f_tiny_file)
        {
            f_v2->rows = f_tiny_file->getRows();
            f_v2->bytes = f_tiny_file->getBytes();
            f_v2->schema = f_tiny_file->getSchema();
            f_v2->data_page_id = f_tiny_file->getDataPageId();
        }
        else
        {
            throw Exception("Unexpected column file type", ErrorCodes::LOGICAL_ERROR);
        }

        column_files_v2.push_back(std::shared_ptr<ColumnStableFile_V2>(f_v2));
    }
    return column_files_v2;
}

inline void serializeColumnFile_V2(const ColumnStableFile_V2 & column_file, const BlockPtr & schema, WriteBuffer & buf)
{
    writeIntBinary(column_file.rows, buf);
    writeIntBinary(column_file.bytes, buf);
    column_file.delete_range.serialize(buf);
    writeIntBinary(column_file.data_page_id, buf);
    if (schema)
    {
        writeIntBinary((UInt32)schema->columns(), buf);
        for (auto & col : *column_file.schema)
        {
            writeIntBinary(col.column_id, buf);
            writeStringBinary(col.name, buf);
            writeStringBinary(col.type->getName(), buf);
        }
    }
    else
    {
        writeIntBinary((UInt32)0, buf);
    }
}

void serializeColumnFiles_V2(WriteBuffer & buf, const ColumnStableFiles_V2 & column_files)
{
    writeIntBinary(column_files.size(), buf);
    BlockPtr last_schema;
    for (auto & column_file : column_files)
    {
        // Do not encode the schema if it is the same as previous one.
        if (column_file->isDeleteRange())
            serializeColumnFile_V2(*column_file, nullptr, buf);
        else
        {
            if (unlikely(!column_file->schema))
                throw Exception("A data pack without schema", ErrorCodes::LOGICAL_ERROR);
            if (column_file->schema != last_schema)
            {
                serializeColumnFile_V2(*column_file, column_file->schema, buf);
                last_schema = column_file->schema;
            }
            else
            {
                serializeColumnFile_V2(*column_file, nullptr, buf);
            }
        }
    }
}

void serializeColumnStableFiles_V2(WriteBuffer & buf, const ColumnStableFiles & column_files)
{
    serializeColumnFiles_V2(buf, transformSaved_V3_to_V2(column_files));
}

inline ColumnStableFile_V2Ptr deserializeColumnStableFile_V2(ReadBuffer & buf, UInt64 version)
{
    auto column_file = std::make_shared<ColumnStableFile_V2>();
    readIntBinary(column_file->rows, buf);
    readIntBinary(column_file->bytes, buf);
    switch (version)
    {
    case DeltaFormat::V1:
    {
        HandleRange range;
        readPODBinary(range, buf);
        column_file->delete_range = RowKeyRange::fromHandleRange(range);
        break;
    }
    case DeltaFormat::V2:
        column_file->delete_range = RowKeyRange::deserialize(buf);
        break;
    default:
        throw Exception("Unexpected version: " + DB::toString(version), ErrorCodes::LOGICAL_ERROR);
    }

    readIntBinary(column_file->data_page_id, buf);

    column_file->schema = deserializeSchema(buf);
    return column_file;
}

ColumnStableFiles deserializeColumnStableFiles_V2(ReadBuffer & buf, UInt64 version)
{
    size_t size;
    readIntBinary(size, buf);
    ColumnStableFiles_V2 column_files;
    BlockPtr last_schema;
    for (size_t i = 0; i < size; ++i)
    {
        auto column_file = deserializeColumnStableFile_V2(buf, version);
        if (!column_file->isDeleteRange())
        {
            if (!column_file->schema)
                column_file->schema = last_schema;
            else
                last_schema = column_file->schema;
        }
        column_files.push_back(column_file);
    }
    return transform_V2_to_V3(column_files);
}

} // namespace DM
} // namespace DB