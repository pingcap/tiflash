#pragma once

#include <Storages/DeltaMerge/ColumnFile.h>

namespace DB
{
namespace DM
{
class ColumnFileSetSnapshot;
using ColumnFileSetSnapshotPtr = std::shared_ptr<ColumnFileSetSnapshot>;
class ColumnFileSetReader;
using ColumnFileSetReaderPtr = std::shared_ptr<ColumnFileSetReader>;

class BlockOrDelete
{
private:
    Block block;
    size_t block_offset;

    RowKeyRange delete_range;

public:
    BlockOrDelete(Block && block_, size_t offset_)
        : block(block_)
        , block_offset(offset_)
    {}
    BlockOrDelete(const RowKeyRange & delete_range_)
        : delete_range(delete_range_)
    {}

    bool isBlock() { return (bool)block; }
    auto & getBlock() { return block; };
    auto getBlockOffset() { return block_offset; }
    auto & getDeleteRange() { return delete_range; }
};

using BlockOrDeletes = std::vector<BlockOrDelete>;

class ColumnFileSetSnapshot : public std::enable_shared_from_this<ColumnFileSetSnapshot>
    , private boost::noncopyable
{
    friend class MemTableSet;
    friend class ColumnStableFileSet;
private:
    StorageSnapshotPtr storage_snap;

    ColumnFiles column_files;
    size_t rows;
    size_t bytes;
    size_t deletes;

    bool is_common_handle;
    size_t rowkey_column_size;

public:
    ColumnFileSetSnapshot(StorageSnapshotPtr storage_snap_)
        :storage_snap{storage_snap_}
    {}

    ColumnFileSetSnapshotPtr clone()
    {
        auto c = std::make_shared<ColumnFileSetSnapshot>(storage_snap);
        c->storage_snap = storage_snap;
        c->column_files = column_files;
        c->rows = rows;
        c->bytes = bytes;
        c->deletes = deletes;
        c->is_common_handle = is_common_handle;
        c->rowkey_column_size = rowkey_column_size;

        return c;
    }

    ColumnFiles & getColumnFiles() { return column_files; }

    size_t getColumnFilesCount() const { return column_files.size(); }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    RowKeyRange getSquashDeleteRange() const;

    const auto & getStorageSnapshot() { return storage_snap; }
};

class ColumnFileSetReader
{
private:
    ColumnFileSetSnapshotPtr snapshot;

    // The columns expected to read. Note that we will do reading exactly in this column order.
    ColumnDefinesPtr col_defs;
    RowKeyRange segment_range;

    // The row count of each pack. Cache here to speed up checking.
    std::vector<size_t> column_file_rows;
    // The cumulative rows of packs. Used to fast locate specific packs according to rows offset by binary search.
    std::vector<size_t> column_file_rows_end;

    std::vector<ColumnFileReaderPtr> column_file_readers;

private:
    Block readPKVersion(size_t offset, size_t limit);

public:
    ColumnFileSetReader(const DMContext & context_,
                   const ColumnFileSetSnapshotPtr & snapshot_,
                   const ColumnDefinesPtr & col_defs_,
                   const RowKeyRange & segment_range_);

    // Use for DeltaMergeBlockInputStream to read rows from MemTableSet to do full compaction with other layer.
    // This method will check whether offset and limit are valid. It only return those valid rows.
    size_t readRows(MutableColumns & output_columns, size_t offset, size_t limit, const RowKeyRange * range);

    void getPlaceItems(BlockOrDeletes & place_items, size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end);

    bool shouldPlace(const DMContext & context,
                     const RowKeyRange & relevant_range,
                     UInt64 max_version,
                     size_t placed_rows);
};
}
}