#pragma once

#include <Storages/DeltaMerge/Delta/DeltaPack.h>

namespace DB
{
namespace DM
{

/// A delta pack which contains a Block. It can be a cache pack (similar to mem-table in LSM-Tree)
/// or a pack with data stored in the "log" area of PageStorage.
class DeltaPackBlock : public DeltaPack
{
    friend class DPBlockReader;

private:
    BlockPtr schema;

    UInt64 rows  = 0;
    UInt64 bytes = 0;

    // The id of data page which stores the data of this pack.
    PageId data_page_id = 0;

    // The members below are not serialized.

    // This pack cannot append any more data.
    bool disable_append = false;
    // The cache data in memory.
    CachePtr cache;
    // Used to map column id to column instance in a Block.
    ColIdToOffset colid_to_offset;

private:
    DeltaPackBlock(const BlockPtr & schema_) : schema(schema_)
    {
        colid_to_offset.clear();
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    void fillColumns(const PageReader & page_reader, const ColumnDefines & col_defs, size_t col_count, Columns & result) const;

    std::pair<DataTypePtr, MutableColumnPtr> getDataTypeAndEmptyColumn(ColId column_id) const
    {
        // Note that column_id must exist
        auto index    = colid_to_offset.at(column_id);
        auto col_type = schema->getByPosition(index).type;
        return {col_type, col_type->createColumn()};
    }

public:
    /// Write the block into PageStorage, and return the page id which references the data page.
    static PageId writePackData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs);

    static DeltaPackPtr writePack(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs);

    /// Create a new pack instance from a persisted page.
    static DeltaPackPtr createPackWithDataPage(const BlockPtr & schema, UInt64 rows, UInt64 bytes, PageId data_page_id);

    /// Create an empty empty cache pack. This pack can accept new data appended into later.
    static DeltaPackPtr createCachePack(const BlockPtr & schema);

    DeltaPackBlock(const DeltaPackBlock &) = default;

    DeltaPackBlockPtr cloneWith(PageId new_data_page_id)
    {
        auto dpb = new DeltaPackBlock(*this);
        dpb->setDataPageId(new_data_page_id);
        return std::shared_ptr<DeltaPackBlock>(dpb);
    }

    Type getType() const override { return Type::BLOCK; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    auto getCache() const { return cache; }
    void setCache(const CachePtr & cache_) { cache = cache_; }
    void clearCache() { cache = {}; }
    bool isCached() const { return (bool)(getCache()); }

    /// The schema of this pack. Could be empty, i.e. a DeleteRange does not have a schema.
    BlockPtr getSchema() const { return schema; }
    /// Update the schema object. It is used to reduce the serialization/deserialization of schema objects.
    /// Note that the caller must make sure that the new schema instance is identical to the current one.
    void setSchema(const BlockPtr & v) { schema = v; }
    /// Replace the schema with a new schema, and the new schema intance should be exactly the same as the previous one.
    void resetIdenticalSchema(BlockPtr schema_) { schema = schema_; }


    /// The page id which references the data of this pack.
    PageId getDataPageId() const { return data_page_id; }
    void   setDataPageId(PageId page_id) { data_page_id = page_id; }

    void appendToCache(const Block data, size_t offset, size_t limit, size_t data_bytes);

    void disableAppend() { disable_append = true; }
    bool isAppendable() { return data_page_id == 0 && cache && !disable_append; }

    void removeData(WriteBatches & wbs) const override
    {
        if (data_page_id)
            wbs.removed_log.delPage(data_page_id);
    }

    /// Read a block from cache / disk according to `pack->schema`
    Block readFromCache() const;
    Block readFromDisk(const PageReader & page_reader) const;

    /// Read a block of columns in `column_defines` from cache / disk,
    /// if `pack->schema` is not match with `column_defines`, take good care of ddl cast
    Columns readFromCache(const ColumnDefines & column_defines, size_t col_start, size_t col_end) const;
    Columns readFromDisk(const PageReader & page_reader, const ColumnDefines & column_defines, size_t col_start, size_t col_end) const;

    DeltaPackReaderPtr
    getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    static std::tuple<DeltaPackPtr, BlockPtr> deserializeMetadata(ReadBuffer & buf, const BlockPtr & last_schema);

    String toString() const override
    {
        String s = "{block,rows:" + DB::toString(rows)                 //
            + ",bytes:" + DB::toString(bytes)                          //
            + ",data_page_id:" + DB::toString(data_page_id)            //
            + ",saved:" + DB::toString(saved)                          //
            + ",disable_append:" + DB::toString(disable_append)        //
            + ",schema:" + (schema ? schema->dumpStructure() : "none") //
            + ",cache_block:" + (cache ? cache->block.dumpStructure() : "none") + "}";
        return s;
    }
};

class DPBlockReader : public DeltaPackReader
{
private:
    const DeltaPackBlock &   pack;
    const StorageSnapshotPtr storage_snap;
    const ColumnDefinesPtr   col_defs;

    Columns cols_data_cache;
    bool    read_done = false;

public:
    DPBlockReader(const DeltaPackBlock &     pack_,
                  const StorageSnapshotPtr & storage_snap_,
                  const ColumnDefinesPtr &   col_defs_,
                  const Columns &            cols_data_cache_)
        : pack(pack_), storage_snap(storage_snap_), col_defs(col_defs_), cols_data_cache(cols_data_cache_)
    {
    }

    DPBlockReader(const DeltaPackBlock & pack_, const StorageSnapshotPtr & storage_snap_, const ColumnDefinesPtr & col_defs_)
        : pack(pack_), storage_snap(storage_snap_), col_defs(col_defs_)
    {
    }

    /// This is a ugly hack to fast return PK & Version column.
    ColumnPtr getPKColumn();
    ColumnPtr getVersionColumn();

    size_t readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range) override;

    Block readNextBlock() override;

    DeltaPackReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs) override;
};

} // namespace DM
} // namespace DB
