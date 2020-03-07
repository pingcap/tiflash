#include <ext/scope_guard.h>

#include <DataTypes/isSupportedDataTypeCast.h>
#include <Functions/FunctionHelpers.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>
#include <Storages/DeltaMerge/HandleFilter.h>

namespace DB
{
namespace DM
{

static constexpr size_t PACK_SERIALIZE_BUFFER_SIZE = 65536;

const Int64 DeltaValueSpace::CURRENT_VERSION = 1;

using BlockPtr       = DeltaValueSpace::BlockPtr;
using Snapshot       = DeltaValueSpace::Snapshot;
using SnapshotPtr    = std::shared_ptr<Snapshot>;
using PageReadFields = PageStorage::PageReadFields;

// ================================================
// Serialize / deserialize
// ================================================

inline void serializePack(const Pack & pack, const BlockPtr & schema, WriteBuffer & buf)
{
    writeIntBinary(pack.rows, buf);
    writeIntBinary(pack.bytes, buf);
    writePODBinary(pack.delete_range, buf);
    writeIntBinary(pack.data_page, buf);
    if (schema)
    {
        writeIntBinary((UInt32)schema->columns(), buf);
        for (auto & col : *pack.schema)
        {
            writeIntBinary(col.column_id, buf);
            writeStringBinary(col.name, buf);
            writeStringBinary(col.type->getName(), buf);
        }
    }
    else
    {
        writeIntBinary((UInt32)0, buf);
    }
}

inline PackPtr deserializePack(ReadBuffer & buf)
{
    auto pack   = std::make_shared<Pack>();
    pack->saved = true; // Must be true, otherwise it should not be here.
    readIntBinary(pack->rows, buf);
    readIntBinary(pack->bytes, buf);
    readPODBinary(pack->delete_range, buf);
    readIntBinary(pack->data_page, buf);
    UInt32 column_size;
    readIntBinary(column_size, buf);
    if (column_size != 0)
    {
        auto schema = std::make_shared<Block>();
        for (size_t i = 0; i < column_size; ++i)
        {
            Int64  column_id;
            String name;
            String type_name;
            readIntBinary(column_id, buf);
            readStringBinary(name, buf);
            readStringBinary(type_name, buf);
            schema->insert(ColumnWithTypeAndName({}, DataTypeFactory::instance().get(type_name), name, column_id));
        }
        pack->setSchema(schema);
    }
    return pack;
}

void serializeSavedPacks(WriteBuffer & buf, const Packs & packs)
{
    size_t saved_packs = std::find_if(packs.begin(), packs.end(), [](const PackPtr & p) { return !p->isSaved(); }) - packs.begin();

    writeIntBinary(DeltaValueSpace::CURRENT_VERSION, buf); // Add binary version
    writeIntBinary(saved_packs, buf);
    BlockPtr last_schema;

    for (auto & pack : packs)
    {
        if (!pack->isSaved())
            break;
        // Do not encode the schema if it is the same as previous one.
        if (pack->isDeleteRange())
            serializePack(*pack, nullptr, buf);
        else
        {
            if (unlikely(!pack->schema))
                throw Exception("A data pack without schema: " + pack->toString(), ErrorCodes::LOGICAL_ERROR);
            if (pack->schema != last_schema)
            {
                serializePack(*pack, pack->schema, buf);
                last_schema = pack->schema;
            }
            else
            {
                serializePack(*pack, nullptr, buf);
            }
        }
    }
}

Packs deserializePacks(ReadBuffer & buf)
{
    // Check binary version
    UInt64 version;
    readIntBinary(version, buf);
    if (version != DeltaValueSpace::CURRENT_VERSION)
        throw Exception("Pack binary version not match: " + DB::toString(version), ErrorCodes::LOGICAL_ERROR);
    size_t size;
    readIntBinary(size, buf);
    Packs    packs;
    BlockPtr last_schema;
    for (size_t i = 0; i < (size_t)size; ++i)
    {
        auto pack = deserializePack(buf);
        if (!pack->isDeleteRange())
        {
            if (!pack->schema)
                pack->setSchema(last_schema);
            else
                last_schema = pack->schema;
        }
        packs.push_back(pack);
    }
    return packs;
}

using BufferAndSize = std::pair<ReadBufferPtr, size_t>;
void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress)
{
    CompressionMethod method = compress ? CompressionMethod::LZ4 : CompressionMethod::NONE;

    CompressedWriteBuffer compressed(buf, CompressionSettings(method));
    type->serializeBinaryBulkWithMultipleStreams(column, //
                                                 [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                 offset,
                                                 limit,
                                                 true,
                                                 {});
    compressed.next();
}

void deserializeColumn(IColumn & column, const DataTypePtr & type, const ByteBuffer & data_buf, size_t rows)
{
    ReadBufferFromMemory buf(data_buf.begin(), data_buf.size());
    CompressedReadBuffer compressed(buf);
    type->deserializeBinaryBulkWithMultipleStreams(column, //
                                                   [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                   rows,
                                                   (double)(data_buf.size()) / rows,
                                                   true,
                                                   {});
}

String packsString(const Packs & packs)
{
    String packs_info = "[";
    for (auto & p : packs)
    {
        packs_info += (p->isDeleteRange() ? "DEL" : "INS_" + DB::toString(p->rows)) + (p->isSaved() ? "_S," : "_N,");
    }
    if (!packs.empty())
        packs_info.erase(packs_info.size() - 1);
    packs_info += "]";
    return packs_info;
}

// ================================================
// Private methods
// ================================================

BlockPtr DeltaValueSpace::lastSchema()
{
    for (auto it = packs.rbegin(); it != packs.rend(); ++it)
    {
        if ((*it)->schema)
            return (*it)->schema;
    }
    return {};
}

Block readPackFromCache(const PackPtr & pack)
{
    std::scoped_lock lock(pack->cache->mutex);

    auto &         cache_block = pack->cache->block;
    MutableColumns columns     = cache_block.cloneEmptyColumns();
    for (size_t i = 0; i < cache_block.columns(); ++i)
        columns[i]->insertRangeFrom(*cache_block.getByPosition(i).column, pack->cache_offset, pack->rows);
    return cache_block.cloneWithColumns(std::move(columns));
}

Columns readPackFromCache(const PackPtr & pack, const ColumnDefines & column_defines, size_t col_start, size_t col_end)
{
    // TODO: should be able to use cache data directly, without copy.
    std::scoped_lock lock(pack->cache->mutex);

    auto &  cache_block = pack->cache->block;
    Columns columns;
    for (size_t i = col_start; i < col_end; ++i)
    {
        auto & col = column_defines[i];
        auto   it  = pack->colid_to_offset.find(col.id);
        if (it == pack->colid_to_offset.end())
        {
            // TODO: support DDL.
            throw Exception("Cannot find column with id" + DB::toString(col.id));
        }
        else
        {
            auto col_offset = it->second;
            auto col_data   = col.type->createColumn();
            col_data->insertRangeFrom(*cache_block.getByPosition(col_offset).column, pack->cache_offset, pack->rows);
            columns.push_back(std::move(col_data));
        }
    }
    return columns;
}

Block readPackFromDisk(const PackPtr & pack, const PageReader & page_reader)
{
    auto & schema = *pack->schema;

    PageReadFields fields;
    fields.first = pack->data_page;
    for (size_t i = 0; i < schema.columns(); ++i)
        fields.second.push_back(i);

    auto page_map = page_reader.read({fields});
    auto page     = page_map[pack->data_page];

    auto columns = schema.cloneEmptyColumns();

    if (unlikely(columns.size() != page.fieldSize()))
        throw Exception("Column size and field size not the same");

    for (size_t index = 0; index < schema.columns(); ++index)
    {
        auto   data_buf = page.getFieldData(index);
        auto & type     = schema.getByPosition(index).type;
        auto & column   = columns[index];
        deserializeColumn(*column, type, data_buf, pack->rows);
    }

    return schema.cloneWithColumns(std::move(columns));
}

Columns readPackFromDisk(const PackPtr &       pack, //
                         const PageReader &    page_reader,
                         const ColumnDefines & column_defines,
                         size_t                col_start,
                         size_t                col_end)
{
    PageReadFields fields;
    fields.first = pack->data_page;
    for (size_t index = col_start; index < col_end; ++index)
    {
        auto col_id = column_defines[index].id;
        auto it     = pack->colid_to_offset.find(col_id);
        if (it == pack->colid_to_offset.end())
            // TODO: support DDL.
            throw Exception("Cannot find column with id" + DB::toString(col_id));
        else
        {
            auto col_index = it->second;
            fields.second.push_back(col_index);
        }
    }

    auto page_map = page_reader.read({fields});
    Page page     = page_map[pack->data_page];

    Columns columns;
    for (size_t index = col_start; index < col_end; ++index)
    {
        auto col_id    = column_defines[index].id;
        auto col_index = pack->colid_to_offset[col_id];
        auto data_buf  = page.getFieldData(col_index);

        auto & cd  = column_defines[index];
        auto   col = cd.type->createColumn();
        deserializeColumn(*col, cd.type, data_buf, pack->rows);

        columns.push_back(std::move(col));
    }

    return columns;
}

void DeltaValueSpace::setUp()
{
    for (auto & pack : packs)
    {
        rows += pack->rows;
        bytes += pack->bytes;
        deletes += pack->isDeleteRange();
        if (!pack->isSaved())
        {
            unsaved_rows += pack->rows;
            unsaved_deletes += pack->isDeleteRange();
        }
    }
}

void DeltaValueSpace::checkNewPacks(const Packs & new_packs)
{
    if constexpr (!DM_RUN_CHECK)
        return;
    size_t new_rows    = 0;
    size_t new_deletes = 0;

    bool seen_unsaved = false;
    bool ok           = true;
    for (auto & pack : new_packs)
    {
        if (pack->isSaved() && seen_unsaved)
        {
            ok = false;
            break;
        }
        seen_unsaved |= !pack->isSaved();

        new_rows += pack->rows;
        new_deletes += pack->isDeleteRange();
    }
    if (unlikely(!ok || new_rows != rows || new_deletes != deletes))
    {
        LOG_ERROR(log, "Rows and deletes check failed. Current packs: " << packsString(packs) << ", new packs: " << packsString(new_packs));
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }
}

// ================================================
// Public methods
// ================================================

DeltaValueSpace::DeltaValueSpace(PageId id_, const Packs & packs_) : id(id_), packs(packs_), log(&Logger::get("DeltaValueSpace"))
{
    setUp();
}

void DeltaValueSpace::restore(DMContext & context)
{
    Page                 page = context.storage_pool.meta().read(id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    packs = deserializePacks(buf);

    setUp();
}

void DeltaValueSpace::saveMeta(WriteBatches & wbs) const
{
    MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
    // Only serialize saved packs.
    serializeSavedPacks(buf, packs);
    auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

Packs DeltaValueSpace::checkHeadAndCloneTail(DMContext &         context,
                                             const HandleRange & target_range,
                                             const Packs &       head_packs,
                                             WriteBatches &      wbs) const
{
    if (head_packs.size() > packs.size())
    {
        LOG_ERROR(log,
                  info() << ", Delta  Check head packs failed, unexpected size. head_packs: " << packsString(head_packs)
                         << ", packs: " << packsString(packs));
        throw Exception("Check head packs failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    auto it_1 = head_packs.begin();
    auto it_2 = packs.begin();
    for (; it_1 != head_packs.end() && it_2 != packs.end(); ++it_1, ++it_2)
    {
        if (*it_1 != *it_2 || (*it_1)->rows != (*it_2)->rows)
        {
            LOG_ERROR(log,
                      simpleInfo() << ", Delta  Check head packs failed, unexpected size. head_packs: " << packsString(head_packs)
                                   << ", packs: " << packsString(packs));
            throw Exception("Check head packs failed", ErrorCodes::LOGICAL_ERROR);
        }
    }

    Packs tail_clone;
    for (; it_2 != packs.end(); ++it_2)
    {
        // Clone a new pack, and column pages are referenced to the old.
        auto & pack     = *it_2;
        auto   new_pack = std::make_shared<Pack>(*pack);
        if (pack->isDeleteRange())
        {
            new_pack->delete_range = pack->delete_range.shrink(target_range);
            if (!new_pack->delete_range.none())
                tail_clone.push_back(new_pack);
        }
        else
        {
            if (pack->data_page != 0)
            {
                auto new_page_id = context.storage_pool.newLogPageId();
                wbs.log.putRefPage(new_page_id, pack->data_page);
                new_pack->data_page = new_page_id;
            }

            tail_clone.push_back(new_pack);
        }
    }

    return tail_clone;
}

size_t DeltaValueSpace::getTotalCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_rows = 0;
    CachePtr         _last_cache;
    for (auto & pack : packs)
    {
        if (pack->cache && pack->cache != _last_cache)
        {
            cache_rows += pack->cache->block.rows();
        }
        _last_cache = pack->cache;
    }
    return cache_rows;
}

size_t DeltaValueSpace::getTotalCacheBytes() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_bytes = 0;
    CachePtr         _last_cache;
    for (auto & pack : packs)
    {
        if (pack->cache && pack->cache != _last_cache)
        {
            cache_bytes += pack->cache->block.bytes();
        }
        _last_cache = pack->cache;
    }
    return cache_bytes;
}

size_t DeltaValueSpace::getValidCacheRows() const
{
    std::scoped_lock lock(mutex);
    size_t           cache_rows = 0;
    for (auto & pack : packs)
    {
        if (pack->isCached())
            cache_rows += pack->rows;
    }
    return cache_rows;
}

void DeltaValueSpace::recordRemovePacksPages(WriteBatches & wbs) const
{
    for (auto & pack : packs)
    {
        wbs.removed_log.delPage(pack->data_page);
    }
}

PageId DeltaValueSpace::writePackData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs)
{
    auto page_id = context.storage_pool.newLogPageId();

    MemoryWriteBuffer write_buf;
    PageFieldSizes    col_data_sizes;
    for (auto & col : block)
    {
        auto last_buf_size = write_buf.count();
        serializeColumn(write_buf, *col.column, col.type, offset, limit, true);
        col_data_sizes.push_back(write_buf.count() - last_buf_size);
    }

    auto data_size = write_buf.count();
    auto buf       = write_buf.tryGetReadBuffer();
    wbs.log.putPage(page_id, 0, buf, data_size, col_data_sizes);

    return page_id;
}

PackPtr DeltaValueSpace::writePack(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs)
{
    auto pack       = std::make_shared<Pack>();
    pack->rows      = limit;
    pack->bytes     = block.bytes() * ((double)limit / block.rows());
    pack->data_page = writePackData(context, block, offset, limit, wbs);
    pack->setSchema(std::make_shared<Block>(block.cloneEmpty()));

    return pack;
}

bool DeltaValueSpace::appendToDisk(DMContext & /*context*/, const PackPtr & pack)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    auto last_schema = lastSchema();
    if (last_schema && checkSchema(*pack->schema, *last_schema))
        pack->schema = last_schema;

    packs.push_back(pack);

    rows += pack->rows;
    bytes += pack->bytes;
    unsaved_rows += pack->rows;

    return true;
}

bool DeltaValueSpace::appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit)
{
    std::scoped_lock lock(mutex);

    if (abandoned.load(std::memory_order_relaxed))
        return false;

    // If last pack has a valid cache block, then we will use the cache block;
    // And, if last pack is mutable (haven't been saved to disk yet), we will merge the newly block into last pack.
    // Otherwise, create a new cache block and write into it.

    PackPtr  mutable_pack;
    CachePtr cache;
    if (!packs.empty() && last_cache)
    {
        std::scoped_lock cache_lock(last_cache->mutex);

        auto & last_pack      = packs.back();
        bool   is_overflow    = last_cache->block.rows() >= context.delta_cache_limit_rows;
        bool   is_same_schema = checkSchema(block, last_cache->block);

        if (!is_overflow && is_same_schema)
        {
            // The last cache block is available
            cache = last_cache;
            if (last_pack->isMutable())
            {
                if (unlikely(last_pack->cache != last_cache))
                    throw Exception("Last mutable pack's cache is not equal to last cache", ErrorCodes::LOGICAL_ERROR);
                mutable_pack = last_pack;
            }
        }
    }

    if (!cache)
    {
        cache      = std::make_shared<Cache>(block);
        last_cache = cache;
    }

    size_t cache_offset;
    {
        std::scoped_lock cache_lock(cache->mutex);

        cache_offset = cache->block.rows();
        for (size_t i = 0; i < cache->block.columns(); ++i)
        {
            auto & col               = block.getByPosition(i).column;
            auto & cache_col         = *cache->block.getByPosition(i).column;
            auto * mutable_cache_col = const_cast<IColumn *>(&cache_col);
            mutable_cache_col->insertRangeFrom(*col, offset, limit);
        }
    }

    size_t append_bytes = block.bytes() * ((double)limit / block.rows());
    if (mutable_pack)
    {
        // Merge into last pack.
        mutable_pack->rows += limit;
        mutable_pack->bytes += append_bytes;
    }
    else
    {
        // Create a new pack.
        auto pack          = std::make_shared<Pack>();
        pack->rows         = limit;
        pack->bytes        = append_bytes;
        pack->cache        = cache;
        pack->cache_offset = cache_offset;

        auto last_schema = lastSchema();
        if (last_schema && checkSchema(block, *last_schema))
            pack->setSchema(last_schema);
        else
            pack->setSchema(std::make_shared<Block>(block.cloneEmpty()));

        packs.push_back(pack);
    }

    rows += limit;
    bytes += append_bytes;
    unsaved_rows += limit;

    return true;
}

bool DeltaValueSpace::appendDeleteRange(DMContext & /*context*/, const HandleRange & delete_range)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    auto pack          = std::make_shared<Pack>();
    pack->delete_range = delete_range;
    packs.push_back(pack);

    ++deletes;
    ++unsaved_deletes;

    return true;
}

struct FlushPackTask
{
    FlushPackTask(const PackPtr & pack_) : pack(pack_) {}

    PackPtr pack;
    PageId  data_page = 0;
};
using FlushPackTasks = std::vector<FlushPackTask>;

bool DeltaValueSpace::flush(DMContext & context)
{
    LOG_DEBUG(log, info() << ", Flush start");

    FlushPackTasks tasks;
    WriteBatches   wbs(context.storage_pool);

    size_t flush_rows    = 0;
    size_t flush_deletes = 0;
    {
        /// Prepare data which will be written to disk.
        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, simpleInfo() << "Flush stop because abandoned");
            return false;
        }

        size_t total_rows    = 0;
        size_t total_deletes = 0;
        for (auto & pack : packs)
        {
            if (unlikely(!tasks.empty() && pack->isSaved()))
            {
                String msg = "Pack should not already saved, because previous packs are not saved.";

                LOG_ERROR(log, simpleInfo() << msg << " Packs: " << packsString(packs));
                throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
            }

            if (!pack->isSaved())
            {
                auto & task = tasks.emplace_back(pack);
                // We only write the pack's data if it is not a delete range, and it's data haven't been saved.
                // Otherwise, simply save it's metadata is enough.
                if (pack->isMutable())
                {
                    if (unlikely(!pack->cache))
                        throw Exception("Mutable pack does not have cache", ErrorCodes::LOGICAL_ERROR);
                    task.data_page = writePackData(context, pack->cache->block, pack->cache_offset, pack->rows, wbs);
                }
                flush_rows += pack->rows;
                flush_deletes += pack->isDeleteRange();
            }
            total_rows += pack->rows;
            total_deletes += pack->isDeleteRange();
        }

        if (unlikely(flush_rows != unsaved_rows || flush_deletes != unsaved_deletes || total_rows != rows || total_deletes != deletes))
            throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);

        // Must remove the last_cache, so that later append operations won't append to last pack which we are flushing.
        last_cache = {};
    }

    // No update, return successfully.
    if (tasks.empty())
    {
        LOG_DEBUG(log, simpleInfo() << " Nothing to flush");
        return true;
    }

    {
        /// Write prepared data to disk.
        wbs.writeLogAndData();
    }

    {
        /// If this instance is still valid, then commit.
        std::scoped_lock lock(mutex);

        if (abandoned.load(std::memory_order_relaxed))
        {
            // Delete written data.
            wbs.setRollback();
            LOG_DEBUG(log, simpleInfo() << " Flush stop because abandoned");
            return false;
        }

        Packs::iterator flush_start_point;
        Packs::iterator flush_end_point;

        {
            /// Do some checks before continue, in case other threads do some modifications during current operation,
            /// as we don't always have the lock.

            auto p_it = packs.begin();
            auto t_it = tasks.begin();
            for (; p_it != packs.end(); ++p_it)
            {
                if (*p_it == t_it->pack)
                    break;
            }

            flush_start_point = p_it;

            for (; t_it != tasks.end(); ++t_it, ++p_it)
            {
                if (p_it == packs.end() || *p_it != t_it->pack || (*p_it)->isSaved())
                {
                    // The packs have been modified, or this pack already saved by another thread.
                    // Let's rollback and break up.
                    wbs.rollbackWrittenLogAndData();
                    LOG_DEBUG(log, simpleInfo() << " Stop flush because structure got updated");
                    return false;
                }
            }

            flush_end_point = p_it;
        }

        /// Things look good, let's continue.

        // Create a temporary packs copy, used to generate serialized data.
        // Save the previous saved packs, and the packs we are saving, and the later packs appended during the io operation.
        Packs packs_copy(packs.begin(), flush_start_point);
        for (auto & task : tasks)
        {
            // Use a new pack instance to do the serializing.
            auto shadow = std::make_shared<Pack>(*task.pack);
            // Set saved to true, otherwise it cannot be serialized.
            shadow->saved = true;
            // If it's data have been updated, use the new pages info.
            if (task.data_page != 0)
                shadow->data_page = task.data_page;
            if (task.pack->rows >= context.delta_small_pack_rows)
            {
                // This pack is too large to use cache.
                task.pack->cache        = {};
                task.pack->cache_offset = 0;
            }

            packs_copy.push_back(shadow);
        }
        packs_copy.insert(packs_copy.end(), flush_end_point, packs.end());

        if constexpr (DM_RUN_CHECK)
        {
            size_t check_unsaved_rows    = 0;
            size_t check_unsaved_deletes = 0;
            size_t total_rows            = 0;
            size_t total_deletes         = 0;
            for (auto & pack : packs_copy)
            {
                if (!pack->isSaved())
                {
                    check_unsaved_rows += pack->rows;
                    check_unsaved_deletes += pack->isDeleteRange();
                }
                total_rows += pack->rows;
                total_deletes += pack->isDeleteRange();
            }
            if (unlikely(check_unsaved_rows + flush_rows != unsaved_rows             //
                         || check_unsaved_deletes + flush_deletes != unsaved_deletes //
                         || total_rows != rows                                       //
                         || total_deletes != deletes))
                throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);
        }

        /// Save the new metadata of packs to disk.
        MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
        serializeSavedPacks(buf, packs_copy);
        const auto data_size = buf.count();

        wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
        wbs.writeMeta();

        /// Commit updates in memory.
        packs.swap(packs_copy);

        unsaved_rows -= flush_rows;
        unsaved_deletes -= flush_deletes;

        LOG_DEBUG(log,
                  simpleInfo() << " Flush end. Flushed " << tasks.size() << " packs, " << flush_rows << " rows and " << flush_deletes
                               << " deletes.");
    }

    return true;
}

struct CompackTask
{
    CompackTask() {}

    Packs   to_compact;
    PackPtr result;
};
using CompackTasks = std::vector<CompackTask>;

bool DeltaValueSpace::compact(DMContext & context)
{
    LOG_DEBUG(log, info() << " Compact start");

    bool v = false;
    // Other thread is doing structure update, just return.
    if (!is_updating.compare_exchange_strong(v, true))
    {
        LOG_DEBUG(log, simpleInfo() << " Compact stop because updating");

        return true;
    }
    SCOPE_EXIT({
        bool v = true;
        if (!is_updating.compare_exchange_strong(v, false))
            throw Exception(simpleInfo() + " is expected to be updating", ErrorCodes::LOGICAL_ERROR);
    });

    CompackTasks             tasks;
    PageStorage::SnapshotPtr log_storage_snap;

    {
        /// Prepare compact tasks.

        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, simpleInfo() << " Compact stop because abandoned");
            return false;
        }

        CompackTask task;
        for (auto & pack : packs)
        {
            if (!pack->isSaved())
                break;
            if ((unlikely(pack->isMutable())))
                throw Exception("Saved pack is mutable", ErrorCodes::LOGICAL_ERROR);

            bool small_pack = !pack->isDeleteRange() && pack->rows < context.delta_small_pack_rows;
            bool schema_ok  = task.to_compact.empty() || pack->schema == task.to_compact.back()->schema;
            if (!small_pack || !schema_ok)
            {
                if (task.to_compact.size() >= 2)
                {
                    tasks.push_back(std::move(task));
                    task = {};
                }
                else
                {
                    // Maybe this pack is small, but it cannot be merged with other packs, so also remove it's cache.
                    for (auto & p : task.to_compact)
                        p->cache = {};

                    task = {};
                }
            }

            if (small_pack)
            {
                task.to_compact.push_back(pack);
            }
            else
            {
                // Then this pack's cache should not exist.
                pack->cache = {};
            }
        }
        if (task.to_compact.size() >= 2)
            tasks.push_back(std::move(task));

        if (tasks.empty())
        {
            LOG_DEBUG(log, simpleInfo() << " Nothing to compact");
            return true;
        }

        log_storage_snap = context.storage_pool.log().getSnapshot();
    }

    /// Write generated compact packs' data.

    size_t total_compact_packs = 0;
    size_t total_compact_rows  = 0;

    WriteBatches wbs(context.storage_pool);
    PageReader   reader(context.storage_pool.log(), log_storage_snap);
    for (auto & task : tasks)
    {
        auto & schema          = *(task.to_compact[0]->schema);
        auto   compact_columns = schema.cloneEmptyColumns();

        for (auto & pack : task.to_compact)
        {
            if (unlikely(pack->isDeleteRange()))
                throw Exception("Unexpectedly selected a delete range to compact", ErrorCodes::LOGICAL_ERROR);

            Block  block      = pack->isCached() ? readPackFromCache(pack) : readPackFromDisk(pack, reader);
            size_t block_rows = block.rows();
            for (size_t i = 0; i < schema.columns(); ++i)
                compact_columns[i]->insertRangeFrom(*block.getByPosition(i).column, 0, block_rows);

            wbs.removed_log.delPage(pack->data_page);
        }

        Block compact_block = schema.cloneWithColumns(std::move(compact_columns));
        auto  compact_rows  = compact_block.rows();

        // Note that after compact, caches are no longer exist.

        auto compact_pack = writePack(context, compact_block, 0, compact_rows, wbs);
        compact_pack->setSchema(task.to_compact.front()->schema);
        compact_pack->saved = true;

        wbs.writeLogAndData();
        task.result = compact_pack;

        total_compact_packs += task.to_compact.size();
        total_compact_rows += compact_rows;
    }

    {
        std::scoped_lock lock(mutex);

        /// Check before commit.
        if (abandoned.load(std::memory_order_relaxed))
        {
            wbs.rollbackWrittenLogAndData();
            LOG_DEBUG(log, simpleInfo() << " Stop compact because abandoned");
            return false;
        }

        Packs new_packs;
        auto  old_packs_offset = packs.begin();
        for (auto & task : tasks)
        {
            auto old_it    = old_packs_offset;
            auto locate_it = [&](const PackPtr & pack) {
                for (; old_it != packs.end(); ++old_it)
                {
                    if (*old_it == pack)
                        return old_it;
                }
                return old_it;
            };

            auto start_it = locate_it(task.to_compact.front());
            auto end_it   = locate_it(task.to_compact.back());

            if (unlikely(start_it == packs.end() || end_it == packs.end()))
            {
                LOG_WARNING(log, "Structure has been updated during compact");
                wbs.rollbackWrittenLogAndData();
                LOG_DEBUG(log, simpleInfo() << " Compact stop because structure got updated");
                return false;
            }

            new_packs.insert(new_packs.end(), old_packs_offset, start_it);
            new_packs.push_back(task.result);

            old_packs_offset = end_it + 1;
        }
        new_packs.insert(new_packs.end(), old_packs_offset, packs.end());

        checkNewPacks(new_packs);

        /// Save the new metadata of packs to disk.
        MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
        serializeSavedPacks(buf, new_packs);
        const auto data_size = buf.count();

        wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
        wbs.writeMeta();

        /// Update packs in memory.
        packs.swap(new_packs);

        last_try_compact_packs = std::min(packs.size(), last_try_compact_packs.load());

        LOG_DEBUG(log,
                  simpleInfo() << " Successfully compacted " << total_compact_packs << " packs into " << tasks.size() << " packs, total "
                               << total_compact_rows << " rows.");
    }

    wbs.writeRemoves();

    return true;
}

// ================================================
// DeltaValueSpace::Snapshot
// ================================================

SnapshotPtr DeltaValueSpace::createSnapshot(const DMContext & context, bool is_update)
{
    if (is_update)
    {
        bool v = false;
        // Other thread is doing structure update, just return.
        if (!is_updating.compare_exchange_strong(v, true))
        {
            LOG_DEBUG(log, simpleInfo() << " Stop create snapshot because updating");
            return {};
        }
    }
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return {};

    auto snap          = std::make_shared<Snapshot>();
    snap->is_update    = is_update;
    snap->delta        = this->shared_from_this();
    snap->storage_snap = std::make_shared<StorageSnapshot>(context.storage_pool, true);
    snap->rows         = rows;
    snap->deletes      = deletes;
    snap->packs.reserve(packs.size());

    if (is_update)
    {
        snap->rows -= unsaved_rows;
        snap->deletes -= unsaved_deletes;
    }

    size_t check_rows    = 0;
    size_t check_deletes = 0;
    size_t total_rows    = 0;
    size_t total_deletes = 0;
    for (auto & pack : packs)
    {
        if (!is_update || pack->isSaved())
        {
            auto pack_copy = pack->isMutable() ? std::make_shared<Pack>(*pack) : pack;
            snap->packs.push_back(std::move(pack_copy));

            check_rows += pack->rows;
            check_deletes += pack->isDeleteRange();
        }
        total_rows += pack->rows;
        total_deletes += pack->isDeleteRange();
    }

    if (unlikely(check_rows != snap->rows || check_deletes != snap->deletes || total_rows != rows || total_deletes != deletes))
        throw Exception("Rows and deletes check failed!", ErrorCodes::LOGICAL_ERROR);

    return snap;
}

class DeltaSnapshotInputStream : public IBlockInputStream
{
    DeltaSnapshotPtr delta_snap;
    size_t           next_pack_index = 0;

public:
    DeltaSnapshotInputStream(const DeltaSnapshotPtr & delta_snap_) : delta_snap(delta_snap_) {}

    String getName() const override { return "DeltaSnapshot"; }
    Block  getHeader() const override { return toEmptyBlock(delta_snap->column_defines); }

    Block read() override
    {
        if (next_pack_index >= delta_snap->packs.size())
            return {};
        return delta_snap->read(next_pack_index++);
    }
};

void DeltaValueSpace::Snapshot::prepare(const DMContext & /*context*/, const ColumnDefines & column_defines_)
{
    column_defines = column_defines_;
    pack_rows.reserve(packs.size());
    pack_rows_end.reserve(packs.size());
    packs_data.reserve(packs.size());
    size_t total_rows = 0;
    for (auto & p : packs)
    {
        total_rows += p->rows;
        pack_rows.push_back(p->rows);
        pack_rows_end.push_back(total_rows);
        packs_data.emplace_back();
    }
}

BlockInputStreamPtr DeltaValueSpace::Snapshot::prepareForStream(const DMContext & context, const ColumnDefines & column_defines_)
{
    prepare(context, column_defines_);
    return std::make_shared<DeltaSnapshotInputStream>(this->shared_from_this());
}


std::pair<size_t, size_t> findPack(const std::vector<size_t> & rows_end, size_t find_offset)
{
    auto it_begin = rows_end.begin();
    auto it       = std::upper_bound(it_begin, rows_end.end(), find_offset);
    if (it == rows_end.end())
        return {rows_end.size(), 0};
    else
    {
        auto pack_offset = it == it_begin ? 0 : *(it - 1);
        return {it - it_begin, find_offset - pack_offset};
    }
}

std::pair<size_t, size_t> findPack(const Packs & packs, size_t rows_offset, size_t deletes_offset)
{
    size_t rows_count    = 0;
    size_t deletes_count = 0;
    size_t pack_index    = 0;
    for (; pack_index < packs.size(); ++pack_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {pack_index, 0};
        auto & pack = packs[pack_index];

        if (pack->isDeleteRange())
        {
            if (deletes_count == deletes_offset)
            {
                if (unlikely(rows_count != rows_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");
                return {pack_index, 0};
            }
            ++deletes_count;
        }
        else
        {
            rows_count += pack->rows;
            if (rows_count > rows_offset)
            {
                if (unlikely(deletes_count != deletes_offset))
                    throw Exception("deletes_offset and rows_offset are not matched");

                return {pack_index, pack->rows - (rows_count - rows_offset)};
            }
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception("illegal rows_offset(" + DB::toString(rows_offset) + "), deletes_count(" + DB::toString(deletes_count) + ")");

    return {pack_index, 0};
}

const Columns & DeltaValueSpace::Snapshot::getColumnsOfPack(size_t pack_index, size_t col_num)
{
    auto & columns = packs_data[pack_index];
    if (columns.size() < col_num)
    {
        size_t col_start = columns.size();
        size_t col_end   = col_num;

        auto read_columns = packs[pack_index]->isCached()
            ? readPackFromCache(packs[pack_index], column_defines, col_start, col_end)
            : readPackFromDisk(packs[pack_index], storage_snap->log_reader, column_defines, col_start, col_end);

        columns.insert(columns.end(), read_columns.begin(), read_columns.end());
    }
    return columns;
}

size_t DeltaValueSpace::Snapshot::read(const HandleRange & range, MutableColumns & output_columns, size_t offset, size_t limit)
{
    auto start = std::min(offset, rows);
    auto end   = std::min(offset + limit, rows);
    if (end == start)
        return 0;

    auto [start_pack_index, rows_start_in_start_pack] = findPack(pack_rows_end, start);
    auto [end_pack_index, rows_end_in_end_pack]       = findPack(pack_rows_end, end);

    size_t actually_read = 0;
    size_t pack_index    = start_pack_index;
    for (; pack_index <= end_pack_index && pack_index < packs.size(); ++pack_index)
    {
        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : pack_rows[pack_index];
        size_t rows_in_pack_limit = rows_end_in_pack - rows_start_in_pack;

        // Nothing to read.
        if (rows_start_in_pack == rows_end_in_pack)
            continue;

        auto & columns         = getColumnsOfPack(pack_index, output_columns.size());
        auto & handle_col_data = toColumnVectorData<Handle>(columns[0]);
        if (rows_in_pack_limit == 1)
        {
            if (range.check(handle_col_data[rows_start_in_pack]))
            {
                for (size_t col_index = 0; col_index < output_columns.size(); ++col_index)
                    output_columns[col_index]->insertFrom(*columns[col_index], rows_start_in_pack);

                ++actually_read;
            }
        }
        else
        {
            auto [actual_offset, actual_limit]
                = HandleFilter::getPosRangeOfSorted(range, handle_col_data, rows_start_in_pack, rows_in_pack_limit);

            for (size_t col_index = 0; col_index < output_columns.size(); ++col_index)
                output_columns[col_index]->insertRangeFrom(*columns[col_index], actual_offset, actual_limit);

            actually_read += actual_limit;
        }
    }
    return actually_read;
}

Block DeltaValueSpace::Snapshot::read(size_t col_num, size_t offset, size_t limit)
{
    MutableColumns columns;
    for (size_t i = 0; i < col_num; ++i)
        columns.push_back(column_defines[i].type->createColumn());
    auto actually_read = read(HandleRange::newAll(), columns, offset, limit);
    if (unlikely(actually_read != limit))
        throw Exception("Expected read " + DB::toString(limit) + " rows, but got " + DB::toString(actually_read));
    Block block;
    for (size_t i = 0; i < col_num; ++i)
    {
        auto cd = column_defines[i];
        block.insert(ColumnWithTypeAndName(std::move(columns[i]), cd.type, cd.name, cd.id));
    }
    return block;
}

Block DeltaValueSpace::Snapshot::read(size_t pack_index)
{
    Block  block;
    auto & pack_columns = getColumnsOfPack(pack_index, column_defines.size());
    for (size_t i = 0; i < column_defines.size(); ++i)
    {
        auto cd = column_defines[i];
        block.insert(ColumnWithTypeAndName(pack_columns[i], cd.type, cd.name, cd.id));
    }
    return block;
}

BlockOrDeletes DeltaValueSpace::Snapshot::getMergeBlocks(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end)
{
    BlockOrDeletes res;

    auto [start_pack_index, rows_start_in_start_pack] = findPack(packs, rows_begin, deletes_begin);
    auto [end_pack_index, rows_end_in_end_pack]       = findPack(packs, rows_end, deletes_end);

    size_t block_rows_start = rows_begin;
    size_t block_rows_end   = rows_begin;

    for (size_t pack_index = start_pack_index; pack_index < packs.size() && pack_index <= end_pack_index; ++pack_index)
    {
        auto & pack = *packs[pack_index];

        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : pack.rows;

        block_rows_end += rows_end_in_pack - rows_start_in_pack;

        if (pack.isDeleteRange() || (pack_index == packs.size() - 1 || pack_index == end_pack_index))
        {
            if (block_rows_end != block_rows_start)
            {
                /// TODO: Here we hard code the first two columns: handle and version
                res.emplace_back(read(2, block_rows_start, block_rows_end - block_rows_start));
            }

            if (pack.isDeleteRange())
            {
                res.emplace_back(pack.delete_range);
            }
            block_rows_start = block_rows_end;
        }
    }

    return res;
}

} // namespace DM
} // namespace DB