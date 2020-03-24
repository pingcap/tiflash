#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/Pack.h>
#include <Storages/DeltaMerge/Delta/Snapshot.h>
#include <Storages/DeltaMerge/HandleFilter.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

namespace DB::DM
{

// ================================================
// DeltaValueSpace::Snapshot
// ================================================
DeltaSnapshotPtr DeltaSnapshot::create(const DMContext & context, const DeltaValueSpacePtr & delta, bool is_update)
{
    if (is_update)
    {
        bool v = false;
        // Other thread is doing structure update, just return.
        if (!delta->is_updating.compare_exchange_strong(v, true))
        {
            LOG_DEBUG(delta->log, delta->simpleInfo() << " Stop create snapshot because updating");
            return {};
        }
    }
    std::scoped_lock lock(delta->mutex);
    if (delta->abandoned.load(std::memory_order_relaxed))
        return {};

    auto snap          = std::make_shared<DeltaSnapshot>();
    snap->is_update    = is_update;
    snap->delta        = delta;
    snap->storage_snap = std::make_shared<StorageSnapshot>(context.storage_pool, true);
    snap->rows         = delta->rows;
    snap->deletes      = delta->deletes;
    snap->packs.reserve(delta->packs.size());

    if (is_update)
    {
        snap->rows -= delta->unsaved_rows;
        snap->deletes -= delta->unsaved_deletes;
    }

    size_t check_rows    = 0;
    size_t check_deletes = 0;
    size_t total_rows    = 0;
    size_t total_deletes = 0;
    for (auto & pack : delta->packs)
    {
        if (!is_update || pack->isSaved())
        {
            auto pack_copy = pack->isAppendable() ? std::make_shared<Pack>(*pack) : pack;
            snap->packs.push_back(std::move(pack_copy));

            check_rows += pack->rows;
            check_deletes += pack->isDeleteRange();
        }
        total_rows += pack->rows;
        total_deletes += pack->isDeleteRange();
    }

    if (unlikely(check_rows != snap->rows || check_deletes != snap->deletes || total_rows != delta->rows
                 || total_deletes != delta->deletes))
    {
        std::stringstream s;
        s << "Rows and deletes check failed! "  //
          << "check_rows:" << check_rows        //
          << ",check_deletes:" << check_deletes //
          << ",total_rows:" << total_rows       //
          << ",total_deletes:" << total_deletes //
          << ",snap->rows:" << snap->rows       //
          << ",snap->deletes:" << snap->deletes //
          << ",delta->rows:" << delta->rows     //
          << ",delta->deletes:" << delta->deletes;
        throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
    }

    return snap;
}

void DeltaSnapshot::prepare(const DMContext & /*context*/, const ColumnDefines & column_defines_)
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

const Columns & DeltaSnapshot::getColumnsOfPack(size_t pack_index, size_t col_num)
{
    // If some columns is already read in this snapshot, we can reuse `packs_data`
    auto & columns = packs_data[pack_index];
    if (columns.size() < col_num)
    {
        size_t col_start = columns.size();
        size_t col_end   = col_num;

        auto &  pack = packs[pack_index];
        Columns read_columns;
        if (pack->isCached())
            read_columns = readPackFromCache(packs[pack_index], column_defines, col_start, col_end);
        else if (pack->data_page != 0)
            read_columns = readPackFromDisk(packs[pack_index], storage_snap->log_reader, column_defines, col_start, col_end);
        else
            throw Exception("Pack is in illegal status: " + pack->toString(), ErrorCodes::LOGICAL_ERROR);

        columns.insert(columns.end(), read_columns.begin(), read_columns.end());
    }
    return columns;
}

size_t DeltaSnapshot::read(const HandleRange & range, MutableColumns & output_columns, size_t offset, size_t limit)
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

Block DeltaSnapshot::read(size_t col_num, size_t offset, size_t limit)
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

Block DeltaSnapshot::read(size_t pack_index)
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

BlockOrDeletes DeltaSnapshot::getMergeBlocks(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end)
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

} // namespace DB::DM
