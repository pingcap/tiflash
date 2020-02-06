#pragma once

#include <common/logger_useful.h>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/Pack.h>
#include <Storages/DeltaMerge/PackBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatches.h>

namespace DB
{
namespace DM
{

struct BlockOrDelete
{
    BlockOrDelete() = default;
    BlockOrDelete(Block && block_) : block(block_) {}
    BlockOrDelete(const HandleRange & delete_range_) : delete_range(delete_range_) {}

    Block       block;
    HandleRange delete_range;
};
using BlockOrDeletes = std::vector<BlockOrDelete>;


struct AppendTask
{
    bool   append_cache; // If not append cache, then clear cache.
    size_t remove_packs_back;
    Packs append_packs;
};
using AppendTaskPtr = std::shared_ptr<AppendTask>;

class DiskValueSpace;
using DiskValueSpacePtr = std::shared_ptr<DiskValueSpace>;

class DiskValueSpace
{
public:
    struct OpContext
    {
        static OpContext createForLogStorage(DMContext & context)
        {
            return OpContext{
                .dm_context       = context,
                .data_storage     = context.storage_pool.log(),
                .meta_storage     = context.storage_pool.meta(),
                .gen_data_page_id = std::bind(&StoragePool::newLogPageId, &context.storage_pool),
            };
        }

        static OpContext createForDataStorage(DMContext & context)
        {
            return OpContext{
                .dm_context       = context,
                .data_storage     = context.storage_pool.data(),
                .meta_storage     = context.storage_pool.meta(),
                .gen_data_page_id = std::bind(&StoragePool::newDataPageId, &context.storage_pool),
            };
        }

        const DMContext & dm_context;

        PageStorage & data_storage;
        PageStorage & meta_storage;
        GenPageId     gen_data_page_id;
    };

    DiskValueSpace(bool should_cache_, PageId page_id_, Packs && packs_ = {}, MutableColumnMap && cache_ = {}, size_t cache_packs_ = 0);
    DiskValueSpace(const DiskValueSpace & other);

    /// Called after the instance is created from existing metadata.
    void restore(const OpContext & context);

    AppendTaskPtr     createAppendTask(const OpContext & context, WriteBatches & wbs, const BlockOrDelete & update) const;
    void              applyAppendToWriteBatches(const AppendTaskPtr & task, WriteBatches & wbs);
    DiskValueSpacePtr applyAppendTask(const OpContext & context, const AppendTaskPtr & task, const BlockOrDelete & update);

    /// Write the blocks from sorted_input_stream into underlying storage, the returned packs can be added to
    /// specified value space instance by #setPacks or #appendPackWithCache later.
    static Packs writePacks(const OpContext & context, const BlockInputStreamPtr & sorted_input_stream, WriteBatch & wb);

    static Pack writeDelete(const OpContext & context, const HandleRange & delete_range);

    /// Remove all packs and clear cache.
    void replacePacks(WriteBatch &        meta_wb, //
                       WriteBatch &        removed_wb,
                       Packs &&           new_packs,
                       MutableColumnMap && cache_,
                       size_t              cache_packs_);
    void replacePacks(WriteBatch & meta_wb, WriteBatch & removed_wb, Packs && new_packs);
    void clearPacks(WriteBatch & removed_wb);
    void setPacks(WriteBatch & meta_wb, Packs && new_packs);
    void setPacksAndCache(WriteBatch & meta_wb, Packs && new_packs, MutableColumnMap && cache_, size_t cache_packs_);

    /// Append the pack to this value space, and could cache the block in memory if it is too fragment.
    void appendPackWithCache(const OpContext & context, Pack && pack, const Block & block);

    /// Read the requested packs' data and compact into a block.
    /// The columns of the returned block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns,
               const PageReader &    page_reader,
               size_t                rows_offset,
               size_t                rows_limit,
               std::optional<size_t> reserve_rows = {}) const;

    /// Read the pack data.
    /// The columns of the returned block are guaranteed to be in order of read_columns.
    Block read(const ColumnDefines & read_columns, const PageReader & page_reader, size_t pack_index) const;

    /// The data of returned block is in insert order.
    BlockOrDeletes getMergeBlocks(const ColumnDefine & handle,
                                  const PageReader &   page_reader,
                                  size_t               rows_begin,
                                  size_t               deletes_begin,
                                  size_t               rows_end,
                                  size_t               deletes_end) const;

    DiskValueSpacePtr tryFlushCache(const OpContext & context, WriteBatch & remove_data_wb, bool force = false);

    // TODO: getInputStream can be removed
    PackBlockInputStreamPtr getInputStream(const ColumnDefines & read_columns, const PageReader & page_reader) const;

    DeltaValueSpacePtr getValueSpace(const PageReader &    page_reader, //
                                     const ColumnDefines & read_columns,
                                     const HandleRange &   range,
                                     size_t                rows_limit) const;

    MutableColumnMap cloneCache();
    size_t           cachePacks() { return cache_packs; }

    size_t num_rows() const;
    size_t num_rows(size_t packs_offset, size_t pack_length) const;
    size_t num_deletes() const;
    size_t num_bytes() const;
    size_t num_packs() const;

    size_t cacheRows() const;
    size_t cacheBytes() const;

    PageId         pageId() const { return page_id; }
    const Packs & getPacks() const { return packs; }
    Packs         getPacksBefore(size_t rows, size_t deletes) const;
    Packs         getPacksAfter(size_t rows, size_t deletes) const;

    void check(const PageReader & meta_page_reader, const String & when);

private:
    DiskValueSpacePtr doFlushCache(const OpContext & context, WriteBatch & remove_data_wb);

    size_t rowsFromBack(size_t packs) const;

    /// Return (pack_index, offset_in_pack)
    std::pair<size_t, size_t> findPack(size_t rows) const;
    std::pair<size_t, size_t> findPack(size_t rows, size_t deletes) const;

private:
    bool is_delta_vs;

    // page_id and packs are the only vars needed to persist.
    PageId page_id;
    Packs packs;

    // The cache is mainly used to merge fragment packs.
    MutableColumnMap cache;
    size_t           cache_packs = 0;

    Logger * log;
};

} // namespace DM
} // namespace DB
