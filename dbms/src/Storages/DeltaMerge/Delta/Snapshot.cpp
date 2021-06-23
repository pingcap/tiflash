#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/Pack.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>
#include <Storages/DeltaMerge/HandleFilter.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

namespace DB::DM
{

// ================================================
// DeltaValueSpace::Snapshot
// ================================================
using Snapshot    = DeltaValueSpace::Snapshot;
using SnapshotPtr = std::shared_ptr<Snapshot>;

DeltaValueSpace::Snapshot::~Snapshot()
{
    if (is_update)
    {
        bool v = true;
        if (!delta->is_updating.compare_exchange_strong(v, false))
        {
            Logger * logger = &Logger::get("DeltaValueSpace::Snapshot");
            LOG_ERROR(logger,
                      "!!!=========================delta [" << delta->getId()
                                                            << "] is expected to be updating=========================!!!");
        }
    }
}

// ================================================
// DeltaValueSpace
// ================================================

DeltaValueSpace::SnapshotPtr DeltaValueSpace::createSnapshot(const DMContext & context, bool for_update, CurrentMetrics::Metric type)
{
    if (for_update)
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

    auto snap          = std::make_shared<Snapshot>(type);
    snap->is_update    = for_update;
    snap->delta        = this->shared_from_this();
    snap->storage_snap = std::make_shared<StorageSnapshot>(context.storage_pool, true);
    snap->rows         = rows;
    snap->bytes        = bytes;
    snap->deletes      = deletes;
    snap->packs.reserve(packs.size());

    snap->shared_delta_index = delta_index;

    /// If `for_update` is false, it will create a snapshot with all packs in DeltaValueSpace.
    /// If `for_update` is true, it will create a snapshot with persisted packs.

    if (for_update)
    {
        snap->rows -= unsaved_rows;
        snap->bytes -= unsaved_bytes;
        snap->deletes -= unsaved_deletes;
    }

    size_t check_rows    = 0;
    size_t check_deletes = 0;
    size_t total_rows    = 0;
    size_t total_deletes = 0;
    for (const auto & pack : packs)
    {
        if (!for_update || pack->isSaved())
        {
            // Because flush/compact threads could update the Pack::cache instance during read operation.
            // We better make a copy if cache exists.
            auto pack_copy = pack->isCached() ? std::make_shared<Pack>(*pack) : pack;
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
        for (; next_pack_index < delta_snap->packs.size(); ++next_pack_index)
        {
            if (!(delta_snap->packs[next_pack_index]->isDeleteRange()))
                break;
        }
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
                    throw Exception("rows_count and rows_offset are expected to be equal. pack_index: " + DB::toString(pack_index)
                                    + ", pack_size: " + DB::toString(packs.size()) + ", rows_count: " + DB::toString(rows_count)
                                    + ", rows_offset: " + DB::toString(rows_offset) + ", deletes_count: " + DB::toString(deletes_count)
                                    + ", deletes_offset: " + DB::toString(deletes_offset));
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
                    throw Exception("deletes_count and deletes_offset are expected to be equal. pack_index: " + DB::toString(pack_index)
                                    + ", pack_size: " + DB::toString(packs.size()) + ", rows_count: " + DB::toString(rows_count)
                                    + ", rows_offset: " + DB::toString(rows_offset) + ", deletes_count: " + DB::toString(deletes_count)
                                    + ", deletes_offset: " + DB::toString(deletes_offset));

                return {pack_index, pack->rows - (rows_count - rows_offset)};
            }
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception("illegal rows_offset and deletes_offset. pack_size: " + DB::toString(packs.size())
                        + ", rows_count: " + DB::toString(rows_count) + ", rows_offset: " + DB::toString(rows_offset)
                        + ", deletes_count: " + DB::toString(deletes_count) + ", deletes_offset: " + DB::toString(deletes_offset));

    return {pack_index, 0};
}

const Columns & DeltaValueSpace::Snapshot::getColumnsOfPack(size_t pack_index, size_t col_num)
{
    // If some columns is already read in this snapshot, we can reuse `packs_data`
    auto & columns = packs_data.at(pack_index);
    if (columns.size() < col_num)
    {
        size_t col_start = columns.size();
        size_t col_end   = col_num;

        auto &  pack = packs.at(pack_index);
        Columns read_columns;
        if (pack->isCached())
            read_columns = readPackFromCache(pack, column_defines, col_start, col_end);
        else if (pack->data_page != 0)
            read_columns = readPackFromDisk(pack, storage_snap->log_reader, column_defines, col_start, col_end);
        else
            throw Exception("Pack is in illegal status: " + pack->toString(), ErrorCodes::LOGICAL_ERROR);

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
    for (size_t pack_index = start_pack_index; pack_index <= end_pack_index && pack_index < packs.size(); ++pack_index)
    {
        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack   = pack_index == end_pack_index ? rows_end_in_end_pack : pack_rows[pack_index];
        size_t rows_in_pack_limit = rows_end_in_pack - rows_start_in_pack;

        // Nothing to read.
        if (rows_start_in_pack == rows_end_in_pack)
            continue;

        // TODO: this get the full columns of pack, which may cause unnecessary copying
        auto & columns         = getColumnsOfPack(pack_index, output_columns.size());
        auto & handle_col_data = toColumnVectorData<Handle>(columns[0]); // TODO: Magic number of fixed position of pk
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
        auto & cd = column_defines[i];
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

bool DeltaValueSpace::Snapshot::shouldPlace(const DMContext &   context,
                                            DeltaIndexPtr       my_delta_index,
                                            const HandleRange & segment_range,
                                            const HandleRange & relevant_range,
                                            UInt64              max_version)
{
    auto [placed_rows, placed_delete_ranges] = my_delta_index->getPlacedStatus();

    // Already placed.
    if (placed_rows >= rows && placed_delete_ranges == deletes)
        return false;

    if (relevant_range.all() || relevant_range == segment_range //
        || rows - placed_rows > context.delta_cache_limit_rows  //
        || placed_delete_ranges != deletes)
        return true;

    auto [start_pack_index, rows_start_in_start_pack] = findPack(pack_rows_end, placed_rows);

    for (size_t pack_index = start_pack_index; pack_index < packs.size(); ++pack_index)
    {
        size_t rows_start_in_pack = pack_index == start_pack_index ? rows_start_in_start_pack : 0;
        size_t rows_end_in_pack   = pack_rows[pack_index];

        auto & columns          = getColumnsOfPack(pack_index, /* handle and version */ 2);
        auto & handle_col_data  = toColumnVectorData<Handle>(columns[0]);
        auto & version_col_data = toColumnVectorData<UInt64>(columns[1]);

        for (auto i = rows_start_in_pack; i < rows_end_in_pack; ++i)
        {
            if (version_col_data[i] <= max_version && relevant_range.check(handle_col_data[i]))
                return true;
        }
    }

    return false;
}

} // namespace DB::DM
