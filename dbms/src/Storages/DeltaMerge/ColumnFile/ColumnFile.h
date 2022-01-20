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
static constexpr size_t COLUMN_FILE_SERIALIZE_BUFFER_SIZE = 65536;

struct DMContext;
class ColumnFile;
using ColumnFilePtr = std::shared_ptr<ColumnFile>;
class ColumnInMemoryFile;
class ColumnTinyFile;
class ColumnDeleteRangeFile;
class ColumnBigFile;

using ColumnFiles = std::vector<ColumnFilePtr>;
class ColumnStableFile;
using ColumnStableFilePtr = std::shared_ptr<ColumnStableFile>;
using ColumnStableFiles = std::vector<ColumnStableFilePtr>;
class ColumnFileReader;
using ColumnFileReaderPtr = std::shared_ptr<ColumnFileReader>;

static std::atomic_uint64_t MAX_COLUMN_FILE_ID{0};

class ColumnFile
{
protected:
    UInt64 id;

    bool saved = false;

    ColumnFile()
        : id(++MAX_COLUMN_FILE_ID)
    {}

    virtual ~ColumnFile() = default;

public:
    enum Type : UInt32
    {
        DELETE_RANGE = 1,
        TINY_FILE = 2,
        BIG_FILE = 3,
        INMEMORY_FILE = 4,
    };

    struct Cache
    {
        explicit Cache(const Block & header)
            : block(header.cloneWithColumns(header.cloneEmptyColumns()))
        {}
        explicit Cache(Block && block)
            : block(std::move(block))
        {}

        std::mutex mutex;
        Block block;
    };
    using CachePtr = std::shared_ptr<Cache>;
    using ColIdToOffset = std::unordered_map<ColId, size_t>;

public:
    /// This id is only used to to do equal check in DeltaValueSpace::checkHeadAndCloneTail.
    bool getId() const { return id; }
    /// This pack is already saved to disk or not. Only saved packs can be recovered after reboot.
    /// "saved" can only be true, after the content data and the metadata are all written to disk.
    bool isSaved() const { return saved; }
    void setSaved() { saved = true; }

    virtual size_t getRows() const { return 0; }
    virtual size_t getBytes() const { return 0; };
    virtual size_t getDeletes() const { return 0; };

    virtual Type getType() const = 0;

    /// Is a ColumnInMemoryFile or not.
    bool isInMemoryFile() const { return getType() == Type::INMEMORY_FILE; }
    /// Is a ColumnTinyFile or not.
    bool isTinyFile() const { return getType() == Type::TINY_FILE; }
    /// Is a ColumnDeleteRangeFile or not.
    bool isDeleteRange() const { return getType() == Type::DELETE_RANGE; };
    /// Is a ColumnBigFile or not.
    bool isBigFile() const { return getType() == Type::BIG_FILE; };

    ColumnInMemoryFile * tryToInMemoryFile();
    ColumnTinyFile * tryToTinyFile();
    ColumnDeleteRangeFile * tryToDeleteRange();
    ColumnBigFile * tryToBigFile();

    virtual ColumnFileReaderPtr
    getReader(const DMContext & context, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const = 0;

    /// only ColumnInMemoryFile can be appendable
    virtual bool isAppendable() const { return false; }
    virtual void disableAppend() {}
    virtual bool append(DMContext & /*dm_context*/, const Block & /*data*/, size_t /*offset*/, size_t /*limit*/, size_t /*data_bytes*/)
    {
        throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR);
    }

    /// Put the data's page id into the corresponding WriteBatch.
    /// The actual remove will be done later.
    virtual void removeData(WriteBatches &) const {};

    virtual void serializeMetadata(WriteBuffer & buf, bool save_schema) const = 0;

    virtual String toString() const = 0;
};


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

void serializeSchema(WriteBuffer & buf, const BlockPtr & schema);
BlockPtr deserializeSchema(ReadBuffer & buf);

void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress);
void deserializeColumn(IColumn & column, const DataTypePtr & type, const ByteBuffer & data_buf, size_t rows);

/// Serialize those packs' metadata into buf.
/// Note that this method stop at the first unsaved pack.
void serializeColumnStableFiles(WriteBuffer & buf, const ColumnFiles & column_files);
/// Recreate pack instances from buf.
ColumnFiles deserializeColumnStableFiles(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf);

void serializeColumnStableFiles_V2(WriteBuffer & buf, const ColumnFiles & column_files);
ColumnFiles deserializeColumnStableFiles_V2(ReadBuffer & buf, UInt64 version);

void serializeColumnStableFiles_V3(WriteBuffer & buf, const ColumnFiles & column_files);
ColumnFiles deserializeColumnStableFiles_V3(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf, UInt64 version);


/// Debugging string
String columnFilesToString(const ColumnFiles & column_files);
} // namespace DM
} // namespace DB
