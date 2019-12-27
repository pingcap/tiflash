#pragma once

#include <common/logger_useful.h>
#include <mutex>

#include <Columns/IColumn.h>
#include <Core/Block.h>

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileChunkFilter.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{
namespace DM
{

struct BlockOrDelete
{
    BlockOrDelete() = default;
    BlockOrDelete(Block block_) : block(std::move(block_)) {}
    BlockOrDelete(const HandleRange & delete_range_) : delete_range(delete_range_) {}

    bool isDelete() const { return !block; }

    Block       block;
    HandleRange delete_range;
};
using BlockOrDeletes = std::vector<BlockOrDelete>;

// Chunk info in disk.
using ChunkID = UInt64;
class ChunkMeta
{
public:
    // Binary version of chunk
    using Version = UInt32;
    static const Version CURRENT_VERSION;

public:
    // For data block
    ChunkID  id      = 0;
    DMFileID file_id = 0; // Which DMFile is belong to
    size_t   index   = 0; // The index in DMFile
    size_t   rows    = 0; // Num of rows

    // For delete_range
    HandleRange delete_range;
    bool        is_delete_range = false;

    ChunkMeta() = default;
    ChunkMeta(ChunkID id_, DMFileID file_id_, size_t index_, size_t rows_)
        : id(id_), file_id(file_id_), index(index_), rows(rows_), is_delete_range(false)
    {
    }
    ChunkMeta(HandleRange delete_range_) : delete_range(delete_range_), is_delete_range(true) {}

    bool equals(const ChunkMeta & rhs) const
    {
        if (is_delete_range)
            return rhs.is_delete_range && delete_range == rhs.delete_range;
        else
            return id == rhs.id && file_id == rhs.file_id && index == rhs.index && rows == rhs.rows;
    }

    void             serialize(WriteBuffer & buf) const;
    static ChunkMeta deserialize(ReadBuffer & buf);
};
using ChunkMetas = std::vector<ChunkMeta>;


struct ChunkOrDelete
{
    ChunkOrDelete() = default;
    ChunkOrDelete(ChunkMeta && chunk_) : chunk(std::move(chunk_)) {}
    ChunkOrDelete(HandleRange && delete_range_) : delete_range(std::move(delete_range_)) {}

    ChunkMeta   chunk;
    HandleRange delete_range;
};

class DeltaSpace;
using DeltaSpacePtr = std::shared_ptr<DeltaSpace>;

// TODO: rename this class to DeltaValueSpace?
class DeltaSpace
{
public:
    struct AppendTask
    {
        ChunkMeta append_chunk;
    };
    using AppendTaskPtr = std::shared_ptr<AppendTask>;

public:
    class Values
    {
    public:
        void addBlock(const Block & block, size_t rows)
        {
            blocks.push_back(block);
            sizes.push_back(rows);
        }

        size_t write(MutableColumns & output_columns, size_t offset, size_t limit) const;

    private:
        inline std::pair<size_t, size_t> findChunk(size_t offset) const
        {
            size_t rows = 0;
            for (size_t block_id = 0; block_id < sizes.size(); ++block_id)
            {
                rows += sizes[block_id];
                if (rows > offset)
                    return {block_id, sizes[block_id] - (rows - offset)};
            }
            return {sizes.size(), 0};
        }

    private:
        Blocks              blocks;
        std::vector<size_t> sizes;
    };
    using DeltaValuesPtr = std::shared_ptr<Values>;

public:
    class Snapshot
    {
    public:
        Snapshot(ChunkMetas chunks_, DMFileMap files_) : chunks(std::move(chunks_)), files(std::move(files_)) {}

        DeltaValuesPtr getValues(const ColumnDefines & read_columns, //
                                 const DMContext &     context) const;

        BlockOrDeletes getMergeBlocks(const ColumnDefine & handle, //
                                      size_t               rows_begin,
                                      size_t               deletes_begin,
                                      const DMContext &    context) const;

        size_t numChunks() const;
        size_t numDeletes() const;
        size_t numRows() const;
        size_t numBytes() const;

        // Simply read all data from delta and return a BlocksListInputStream (Just for raw read now)
        BlockInputStreamPtr getInputStream(const ColumnDefines & read_columns, const DMContext & context) const;

        // Just for test
        const ChunkMetas & getChunks() const { return chunks; }

    private:
        Block read(const ColumnDefines & read_columns,
                   size_t                rows_offset,
                   size_t                rows_limit,
                   const DMContext &     context,
                   std::optional<size_t> reserve_rows_ = {}) const;

        using ReadCallback = std::function<void(const Block &, size_t block_rows_offset, size_t block_rows_limit)>;

        /// Read from a specific DMFile
        void readFromFile(DMFileID              file_id, //
                          const ColumnDefines & read_columns,
                          const IndexSetPtr &   indices,
                          const DMContext &     context,
                          const ReadCallback &  callback,
                          std::optional<size_t> rows_offset_ = std::nullopt,
                          std::optional<size_t> rows_limit_  = std::nullopt) const;

        std::pair<size_t, size_t> findChunk(size_t rows_offset, size_t deletes_offset) const;
        std::pair<size_t, size_t> findChunk(size_t rows_offset) const;

    private:
        const ChunkMetas chunks;
        const DMFileMap  files;

        friend class DeltaSpace;
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    SnapshotPtr getSnapshot() const { return std::make_shared<Snapshot>(chunks, files); }

public:
    DeltaSpace(PageId id_, String parent_path_) : id(id_), parent_path(std::move(parent_path_)), log(&Logger::get("DeltaSpace")) {}
    ~DeltaSpace();

    // Generate a new delta(with the same id) and remove the chunks in snap
    DeltaSpacePtr nextGeneration(const SnapshotPtr & snap, WriteBatches & wbs);

    static DeltaSpacePtr
    newRef(const SnapshotPtr & snap, PageId new_delta_id, const String & parent_path, const GenPageId & gen_chunk_id, WriteBatches & wbs);

    /// Serialize
    void saveMeta(WriteBatch & meta_wb);
    /// Restore an instance from existing metadata.
    static DeltaSpacePtr restore(PageId id, const DMContext & context);

    /// For write
    // Append to disk and create task for changes of chunks. Do it without lock since write is heavy.
    AppendTaskPtr createAppendTask(const DMContext & context, const BlockOrDelete & update, WriteBatches & wbs);
    // Apply chunks' changes to wbs, use it to build higher atomic level.
    void applyAppendToWriteBatches(const AppendTaskPtr & task, WriteBatches & wbs);
    // Apply chunks' changes commited in disk, apply changes in memory.
    void applyAppendInMemory(const AppendTaskPtr & task);

    void check(const PageReader & meta_page_reader, const String & when);

    void drop();

public:
    PageId         pageId() const { return id; }
    const String & parentPath() const { return parent_path; }

    size_t numChunks() const
    {
        auto snap = getSnapshot();
        return snap->numChunks();
    }
    size_t numDeletes() const
    {
        auto snap = getSnapshot();
        return snap->numDeletes();
    }
    size_t numRows() const
    {

        auto snap = getSnapshot();
        return snap->numRows();
    }
    size_t numBytes() const
    {
        //FIXME:
        // auto snap = getSnapshot();
        return 0;
    }

private:
    PageId id; // PageId to store this delta's meta in PageStorage.
    String parent_path;

    /// Here we use vector of ChunkMeta. Create a snapshot means copy the vector of ChunkMetas.
    /// We may try to write a list for ChunkMeta for eliminating the copying and manage valid chunks' lifetime by std::shared_ptr
    ChunkMetas chunks;
    // mutable std::shared_mutex read_write_mutex; // We already protect delta in higher level now.

    DMFileMap files;
    DMFilePtr file_writting = nullptr; // Keep it so that we can easily track the chunk index we are writing to.

    std::unique_ptr<DMFileWriter> writer = nullptr;

    Logger * log;
};

using DeltaSnapshotPtr = DeltaSpace::SnapshotPtr;
using DeltaValuesPtr   = DeltaSpace::DeltaValuesPtr;

} // namespace DM
} // namespace DB
