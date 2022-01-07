#pragma once

#include <Storages/DeltaMerge/ColumnFile.h>

namespace DB
{
namespace DM
{
class ColumnFileSetSnapshot : public std::enable_shared_from_this<ColumnFileSetSnapshot>
    , private boost::noncopyable
{
    friend class MemTableSet;
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

    ColumnFiles & getColumnFiles() { return column_files; }

    size_t getColumnFilesCount() const { return column_files.size(); }
    size_t getRows() const { return rows; }
    size_t getBytes() const { return bytes; }
    size_t getDeletes() const { return deletes; }

    const auto & getStorageSnapshot() { return storage_snap; }
};

using ColumnFileSetSnapshotPtr = std::shared_ptr<ColumnFileSetSnapshot>;

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

public:
    ColumnFileSetReader(const DMContext & context_,
                   const ColumnFileSetSnapshotPtr & snapshot_,
                   const ColumnDefinesPtr & col_defs_,
                   const RowKeyRange & segment_range_);

    // Use for DeltaMergeBlockInputStream to read rows from MemTableSet to do full compaction with other layer.
    // This method will check whether offset and limit are valid. It only return those valid rows.
    size_t readRows(MutableColumns & output_columns, size_t offset, size_t limit, const RowKeyRange * range);
};
}
}