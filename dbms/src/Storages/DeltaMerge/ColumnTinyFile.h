#pragma once

#include <Storages/DeltaMerge/ColumnStableFile.h>

namespace DB
{
namespace DM
{
class ColumnTinyFileReader;

class ColumnTinyFile : public ColumnStableFile
{
    friend class ColumnTinyFileReader;
private:
    BlockPtr schema;
    // TODO: figure out the concurrency problem with this cache
    BlockPtr cache;
    mutable std::mutex mutex;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    // The id of data page which stores the data of this pack.
    PageId data_page_id;

    // The members below are not serialized.

    // Used to map column id to column instance in a Block.
    ColIdToOffset colid_to_offset;

private:
    /// Read a block of columns in `column_defines` from cache / disk,
    /// if `pack->schema` is not match with `column_defines`, take good care of ddl cast
    Columns readFromCache(const ColumnDefines & column_defines, size_t col_start, size_t col_end) const;
    Columns readFromDisk(const PageReader & page_reader, const ColumnDefines & column_defines, size_t col_start, size_t col_end) const;

    void fillColumns(const PageReader & page_reader, const ColumnDefines & col_defs, size_t col_count, Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return schema->getByPosition(index).type;
    }

public:
    ColumnTinyFile(const BlockPtr & schema_, const BlockPtr & cache_, UInt64 rows_, UInt64 bytes_, PageId data_page_id_)
        : schema(schema_),
          cache(cache_),
          rows(rows_),
          bytes(bytes_),
          data_page_id(data_page_id_)
    {
        colid_to_offset.clear();
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    void clearCache()
    {
        std::scoped_lock lock(mutex);
        cache = {};
    }

    /// The schema of this pack. Could be empty, i.e. a DeleteRange does not have a schema.
    BlockPtr getSchema() const { return schema; }
    /// Update the schema object. It is used to reduce the serialization/deserialization of schema objects.
    /// Note that the caller must make sure that the new schema instance is identical to the current one.
    void setSchema(const BlockPtr & v) { schema = v; }
    /// Replace the schema with a new schema, and the new schema intance should be exactly the same as the previous one.
    void resetIdenticalSchema(BlockPtr schema_) { schema = schema_; }

    void removeData(WriteBatches & wbs) const override
    {
        wbs.removed_log.delPage(data_page_id);
    }

    ColumnFileReaderPtr
    getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;
};


class ColumnTinyFileReader : public ColumnFileReader
{
private:
    const ColumnTinyFile & tiny_file;
    const StorageSnapshotPtr storage_snap;
    const ColumnDefinesPtr col_defs;

    Columns cols_data_cache;
    bool read_done = false;

public:
    ColumnTinyFileReader(const ColumnTinyFile & tiny_file_,
                         const StorageSnapshotPtr & storage_snap_,
                             const ColumnDefinesPtr & col_defs_,
                             const Columns & cols_data_cache_)
        : tiny_file(tiny_file_)
        , storage_snap(storage_snap_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
    {
    }

    ColumnTinyFileReader(const ColumnTinyFile & tiny_file_, const StorageSnapshotPtr & storage_snap_, const ColumnDefinesPtr & col_defs_)
        : tiny_file(tiny_file_)
        , storage_snap(storage_snap_)
        , col_defs(col_defs_)
    {
    }

    /// This is a ugly hack to fast return PK & Version column.
    ColumnPtr getPKColumn();
    ColumnPtr getVersionColumn();

    size_t readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range) override;

    Block readNextBlock() override;

    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs) override;
};
}
}
