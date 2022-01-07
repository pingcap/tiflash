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
    // The cache data in memory.
    BlockPtr block;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    // This instance cannot append any more data.
    bool disable_append = false;

    mutable std::mutex mutex;

    // Used to map column id to column instance in a Block.
    ColIdToOffset colid_to_offset;

private:
    void fillColumns(const ColumnDefines & col_defs, size_t col_count, Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return block->getByPosition(index).type;
    }

public:
    explicit ColumnInMemoryFile(const Block & schema)
        : block(std::make_shared<Block>(schema.cloneEmpty()))
    {
        colid_to_offset.clear();
        for (size_t i = 0; i < block->columns(); ++i)
            colid_to_offset.emplace(block->getByPosition(i).column_id, i);
    }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    bool isAppendable() const override
    {
        std::scoped_lock lock(mutex);
        return !disable_append;
    }
    void disableAppend() override
    {
        std::scoped_lock lock(mutex);
        disable_append = true;
    }
    bool append(DMContext & dm_context, const Block & data, size_t offset, size_t limit, size_t data_bytes) override;

    ColumnFileReaderPtr
    getReader(const DMContext & context, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;
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
