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

static constexpr size_t PACK_SERIALIZE_BUFFER_SIZE = 65536;

struct DMContext;
class DeltaPack;
class DeltaPackBlock;
class DeltaPackFile;
class DeltaPackDeleteRange;
class DMFileBlockInputStream;
class DeltaPackReader;

using DeltaPackReaderPtr        = std::shared_ptr<DeltaPackReader>;
using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;
using DeltaPackPtr              = std::shared_ptr<DeltaPack>;
using DeltaPackBlockPtr         = std::shared_ptr<DeltaPackBlock>;
using DeltaPackFilePtr          = std::shared_ptr<DeltaPackFile>;
using DeltaPackDeleteRangePtr   = std::shared_ptr<DeltaPackDeleteRange>;

using ConstDeltaPackPtr = std::shared_ptr<const DeltaPack>;
using DeltaPackBlockPtr = std::shared_ptr<DeltaPackBlock>;
using DeltaPackFilePtr  = std::shared_ptr<DeltaPackFile>;
using DeltaPacks        = std::vector<DeltaPackPtr>;
using DeltaPackBlocks   = std::vector<DeltaPackBlockPtr>;

static std::atomic_uint64_t MAX_PACK_ID{0};

class DeltaPack
{
protected:
    UInt64 id;

    bool saved = false;

    DeltaPack() : id(++MAX_PACK_ID) {}

    virtual ~DeltaPack() = default;

public:
    enum Type : UInt32
    {
        DELETE_RANGE = 1,
        BLOCK        = 2,
        FILE         = 3,
    };

    struct Cache
    {
        Cache(const Block & header) : block(header.cloneWithColumns(header.cloneEmptyColumns())) {}
        Cache(Block && block) : block(std::move(block)) {}

        std::mutex mutex;
        Block      block;
    };

    using CachePtr      = std::shared_ptr<Cache>;
    using ColIdToOffset = std::unordered_map<ColId, size_t>;

public:
    /// This id is only used to to do equal check in DeltaValueSpace::checkHeadAndCloneTail.
    bool getId() const { return id; }
    /// This pack is already saved to disk or not. Only saved packs can be recovered after reboot.
    /// "saved" can only be true, after the content data and the metadata are all written to disk.
    bool isSaved() const { return saved; }
    void setSaved() { saved = true; }

    /// Only DeltaPackBlock could return true.
    virtual bool isDataFlushable() { return false; }

    virtual size_t getRows() const { return 0; }
    virtual size_t getBytes() const { return 0; };
    virtual size_t getDeletes() const { return 0; };

    virtual Type getType() const = 0;

    /// Is a DeltaPackBlock or not.
    bool isBlock() const { return getType() == Type::BLOCK; }
    /// Is a DeltaPackFile or not.
    bool isFile() const { return getType() == Type::FILE; }
    /// Is a DeltaPackDeleteRange or not.
    bool isDeleteRange() const { return getType() == Type::DELETE_RANGE; };

    DeltaPackBlock *       tryToBlock();
    DeltaPackFile *        tryToFile();
    DeltaPackDeleteRange * tryToDeleteRange();

    /// Put the data's page id into the corresponding WriteBatch.
    /// The actual remove will be done later.
    virtual void removeData(WriteBatches &) const {}

    virtual DeltaPackReaderPtr
    getReader(const DMContext & context, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr &) const = 0;

    virtual String toString() const = 0;

    virtual void serializeMetadata(WriteBuffer & buf, bool save_schema) const = 0;
};

class DeltaPackReader
{
public:
    virtual ~DeltaPackReader()                 = default;
    DeltaPackReader()                          = default;
    DeltaPackReader(const DeltaPackReader & o) = delete;

    /// Read data from this reader and store the result into output_cols.
    /// Note that if "range" is specified, then the caller must guarantee that the rows between [rows_offset, rows_offset + rows_limit) are sorted.
    virtual size_t readRows(MutableColumns & /*output_cols*/, size_t /*rows_offset*/, size_t /*rows_limit*/, const RowKeyRange * /*range*/)
    {
        throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR);
    }

    /// This method is only used to read raw data.
    virtual Block readNextBlock() { throw Exception("Unsupported operation", ErrorCodes::LOGICAL_ERROR); }

    /// Create a new reader from current reader with different columns to read.
    virtual DeltaPackReaderPtr createNewReader(const ColumnDefinesPtr & col_defs) = 0;
};

size_t copyColumnsData(
    const Columns & from, const ColumnPtr & pk_col, MutableColumns & to, size_t rows_offset, size_t rows_limit, const RowKeyRange * range);

void     serializeSchema(WriteBuffer & buf, const BlockPtr & schema);
BlockPtr deserializeSchema(ReadBuffer & buf);

void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress);
void deserializeColumn(IColumn & column, const DataTypePtr & type, const ByteBuffer & data_buf, size_t rows);

/// Serialize those packs' metadata into buf.
/// Note that this method stop at the first unsaved pack.
void serializeSavedPacks(WriteBuffer & buf, const DeltaPacks & packs);
/// Recreate pack instances from buf.
DeltaPacks deserializePacks(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf);

void       serializeSavedPacks_v2(WriteBuffer & buf, const DeltaPacks & packs);
DeltaPacks deserializePacks_V2(ReadBuffer & buf, UInt64 version);

void       serializeSavedPacks_V3(WriteBuffer & buf, const DeltaPacks & packs);
DeltaPacks deserializePacks_V3(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf, UInt64 version);

/// Debugging string
String packsToString(const DeltaPacks & packs);

} // namespace DM
} // namespace DB
