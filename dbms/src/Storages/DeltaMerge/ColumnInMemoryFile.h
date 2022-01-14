#pragma once

#include <Storages/DeltaMerge/ColumnFile.h>

namespace DB
{
namespace DM
{

class ColumnInMemoryFileReader;

class ColumnInMemoryFile : public ColumnFile
{
    friend class ColumnInMemoryFileReader;
private:
    BlockPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    // This pack cannot append any more data.
    bool disable_append = false;

    // The cache data in memory.
    CachePtr cache;
    // Used to map column id to column instance in a Block.
    ColIdToOffset colid_to_offset;

private:
    void fillColumns(const ColumnDefines & col_defs, size_t col_count, Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return schema->getByPosition(index).type;
    }

public:
    explicit ColumnInMemoryFile(const Block & schema_)
        : schema(std::make_shared<Block>(schema_.cloneEmpty())),
        cache(std::make_shared<Cache>(schema_))
    {
        colid_to_offset.clear();
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    Type getType() const override { return Type::INMEMORY_FILE; }
    /// The schema of this pack. Could be empty, i.e. a DeleteRange does not have a schema.
    BlockPtr getSchema() const { return schema; }
    /// Update the schema object. It is used to reduce the serialization/deserialization of schema objects.
    /// Note that the caller must make sure that the new schema instance is identical to the current one.
    void setSchema(const BlockPtr & v) { schema = v; }
    /// Replace the schema with a new schema, and the new schema instance should be exactly the same as the previous one.
    void resetIdenticalSchema(BlockPtr schema_) { schema = schema_; }

    CachePtr getCache() { return cache; }

    bool isAppendable() const override
    {
        return !disable_append;
    }
    void disableAppend() override
    {
        disable_append = true;
    }
    bool append(DMContext & dm_context, const Block & data, size_t offset, size_t limit, size_t data_bytes) override;

    ColumnFileReaderPtr
    getReader(const DMContext & context, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;

    Block readDataForFlush() const;

    String toString() const override { return ""; }
};


class ColumnInMemoryFileReader : public ColumnFileReader
{
private:
    const ColumnInMemoryFile & memory_file;
    const ColumnDefinesPtr col_defs;

    Columns cols_data_cache;
    bool read_done = false;

public:
    ColumnInMemoryFileReader(const ColumnInMemoryFile & memory_file_,
                  const ColumnDefinesPtr & col_defs_,
                  const Columns & cols_data_cache_)
        : memory_file(memory_file_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
    {
    }

    ColumnInMemoryFileReader(const ColumnInMemoryFile & memory_file_, const ColumnDefinesPtr & col_defs_)
        : memory_file(memory_file_)
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
