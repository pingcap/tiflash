#pragma once

#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{
namespace DM
{
struct DMContext;
class ColumnFileReader;
using ColumnFileReaderPtr = std::shared_ptr<ColumnFileReader>;

class ColumnFile
{
protected:
    virtual ~ColumnFile() = default;

protected:
    using ColIdToOffset = std::unordered_map<ColId, size_t>;

public:
    virtual bool isAppendable() const { return false; }
    virtual void disableAppend() {}
    virtual bool append(DMContext & /*dm_context*/, const Block & /*data*/, size_t /*offset*/, size_t /*limit*/, size_t /*data_bytes*/)
    {
        throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR);
    }

    virtual size_t getRows() const { return 0; }
    virtual size_t getBytes() const { return 0; };
    virtual size_t getDeletes() const { return 0; };

    virtual ColumnFileReaderPtr
    getReader(const DMContext & context, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const = 0;
};

using ColumnFilePtr = std::shared_ptr<ColumnFile>;
using ColumnFiles = std::vector<ColumnFilePtr>;

class ColumnFileReader
{
public:
    virtual ~ColumnFileReader() = default;
    ColumnFileReader() = default;
    ColumnFileReader(const ColumnFileReader & o) = delete;

    /// Read data from this reader and store the result into output_cols.
    /// Note that if "range" is specified, then the caller must guarantee that the rows between [rows_offset, rows_offset + rows_limit) are sorted.
    virtual size_t readRows(MutableColumns & /*output_cols*/, size_t /*rows_offset*/, size_t /*rows_limit*/, const RowKeyRange * /*range*/)
    {
        throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR);
    }

    /// This method is only used to read raw data.
    virtual Block readNextBlock() { throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR); }

    /// Create a new reader from current reader with different columns to read.
    virtual ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & col_defs) = 0;
};

size_t copyColumnsData(
    const Columns & from,
    const ColumnPtr & pk_col,
    MutableColumns & to,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range);
}
}
