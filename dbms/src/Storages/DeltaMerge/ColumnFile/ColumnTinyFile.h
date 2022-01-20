#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnStableFile.h>

namespace DB
{
namespace DM
{
class ColumnTinyFile;
using ColumnTinyFilePtr = std::shared_ptr<ColumnTinyFile>;

class ColumnTinyFile : public ColumnStableFile
{
    friend class ColumnTinyFileReader;

private:
    BlockPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    // The id of data page which stores the data of this pack.
    PageId data_page_id;

    /// The members below are not serialized.
    // The cache data in memory.
    CachePtr cache;
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
    ColumnTinyFile(const BlockPtr & schema_, UInt64 rows_, UInt64 bytes_, PageId data_page_id_, const CachePtr & cache_ = nullptr)
        : schema(schema_),
          rows(rows_),
          bytes(bytes_),
          data_page_id(data_page_id_),
          cache(cache_)
    {
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    Type getType() const override { return Type::TINY_FILE; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    auto getCache() const { return cache; }
    void clearCache() { cache = {}; }

    /// The schema of this pack. Could be empty, i.e. a DeleteRange does not have a schema.
    BlockPtr getSchema() const { return schema; }

    ColumnTinyFilePtr cloneWith(PageId new_data_page_id)
    {
        return std::make_shared<ColumnTinyFile>(schema, rows, bytes, new_data_page_id, cache);
    }

    ColumnFileReaderPtr
    getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;

    void removeData(WriteBatches & wbs) const override
    {
        wbs.removed_log.delPage(data_page_id);
    }

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    PageId getDataPageId() const { return data_page_id; }

    Block readBlockForMinorCompaction(const PageReader & page_reader) const;

    static ColumnTinyFilePtr writeColumnFile(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs, const BlockPtr & schema = nullptr, const CachePtr & cache = nullptr);

    static PageId writeColumnFileData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs);

    static std::tuple<ColumnStableFilePtr, BlockPtr> deserializeMetadata(ReadBuffer & buf, const BlockPtr & last_schema);

    String toString() const override
    {
        String s = "{tiny_file,rows:" + DB::toString(rows) //
                   + ",bytes:" + DB::toString(bytes) //
                   + ",data_page_id:" + DB::toString(data_page_id) //
                   + ",schema:" + (schema ? schema->dumpStructure() : "none") //
                   + ",cache_block:" + (cache ? cache->block.dumpStructure() : "none") + "}";
        return s;
    }
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
