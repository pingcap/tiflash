#pragma once

#include <Common/Exception.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/HandleFilter.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB
{
namespace DM
{

// Note that the columns in stable input stream and value space must exactly the same, including name, type, and id.
// The first column must be handle column.
template <class DeltaValueSpace, class IndexIterator>
class DeltaMergeBlockInputStream final : public IBlockInputStream, Allocator<false>
{
    static constexpr size_t UNLIMITED = std::numeric_limits<UInt64>::max();

private:
    using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;
    using SharedLock         = std::shared_lock<std::shared_mutex>;

    struct IndexEntry
    {
        UInt64 sid;
        UInt16 type;
        UInt32 count;
        UInt64 value;
    };

    SkippableBlockInputStreamPtr stable_input_stream;

    // How many rows we need to skip before writing stable rows into output.
    // == 0: None
    // > 0 : some rows are ignored by delta tree (index), should not write into output.
    // < 0 : some rows are filtered out by stable filter, should not write into output.
    ssize_t stable_skip = 0;

    DeltaValueSpacePtr delta_value_space;

    IndexEntry * index;
    size_t       index_capacity;
    size_t       index_valid_size;
    size_t       index_pos;

    HandleRange handle_range;

    size_t max_block_size;

    Block  header;
    size_t num_columns;

    size_t use_stable_rows;

    size_t use_delta_offset = 0;
    size_t use_delta_rows   = 0;

    Columns cur_stable_block_columns;
    size_t  cur_stable_block_rows = 0;
    size_t  cur_stable_block_pos  = 0;

    bool stable_done = false;
    bool delta_done  = false;

    // How many times `read` is called.
    size_t num_read = 0;

    Handle last_handle          = N_INF_HANDLE;
    size_t last_handle_pos      = 0;
    size_t last_handle_read_num = 0;

public:
    DeltaMergeBlockInputStream(const SkippableBlockInputStreamPtr & stable_input_stream_,
                               const DeltaValueSpacePtr &           delta_value_space_,
                               IndexIterator                        index_begin,
                               IndexIterator                        index_end,
                               size_t                               index_size_,
                               const HandleRange                    handle_range_,
                               size_t                               max_block_size_)
        : stable_input_stream(stable_input_stream_),
          delta_value_space(delta_value_space_),
          index((IndexEntry *)(alloc(sizeof(IndexEntry) * index_size_))),
          index_capacity(index_size_),
          index_pos(0),
          handle_range(handle_range_),
          max_block_size(max_block_size_)
    {
        header      = stable_input_stream->getHeader();
        num_columns = header.columns();

        // Let's compact the index.
        IndexIterator it  = index_begin;
        size_t        pos = 0;
        while (it != index_end)
        {
            if (pos > 0 && it.getType() == DT_INS)
            {
                auto & prev_index = index[pos - 1];
                if (prev_index.type == DT_INS        //
                    && prev_index.sid == it.getSid() //
                    && prev_index.value + prev_index.count == it.getValue())
                {
                    // Merge current insert entry into previous one.
                    index[pos - 1].count += it.getCount();
                    ++it;
                    continue;
                }
            }

            index[pos] = IndexEntry{
                .sid   = it.getSid(),
                .type  = it.getType(),
                .count = it.getCount(),
                .value = it.getValue(),
            };
            ++pos;
            ++it;
        }
        index_valid_size = pos;

        if (unlikely(it != index_end))
            throw Exception("Index iterator is expected to be equal to index_end");

        if (!index_valid_size)
        {
            use_stable_rows = UNLIMITED;
            delta_done      = true;
        }
        else
        {
            use_stable_rows = index[index_pos].sid;
        }
    }

    ~DeltaMergeBlockInputStream() { free(index, sizeof(IndexEntry) * index_capacity); }

    String getName() const override { return "DeltaMerge"; }
    Block  getHeader() const override { return header; }

    Block read() override
    {
        ++num_read;
        if (finished())
            return {};
        while (!finished())
        {
            MutableColumns columns;
            initOutputColumns(columns);

            size_t limit = max_block_size;
            while (limit)
            {
                if (stable_done)
                {
                    if (delta_done)
                        break;
                    else
                        next<true, false>(columns, limit);
                }
                else
                {
                    if (delta_done)
                        next<false, true>(columns, limit);
                    else
                        next<false, false>(columns, limit);
                }
            }

            // Empty means we are out of data.
            if (limit == max_block_size)
                continue;

            auto result = header.cloneWithColumns(std::move(columns));
            if constexpr (DM_RUN_CHECK)
            {
                auto & handle_column = toColumnVectorData<Handle>(result.getByPosition(0).column);
                for (size_t i = 0; i < handle_column.size(); ++i)
                {
                    if (handle_column[i] < last_handle)
                    {
                        throw Exception("DeltaMerge return wrong result, current handle [" + DB::toString(handle_column[i]) + "]@read["
                                        + DB::toString(num_read) + "]@pos[" + DB::toString(i) + "] is expected >= last handle ["
                                        + DB::toString(last_handle) + "]@read[" + DB::toString(last_handle_read_num) + "]@pos["
                                        + DB::toString(last_handle_pos) + "]");
                    }
                    last_handle          = handle_column[i];
                    last_handle_pos      = i;
                    last_handle_read_num = num_read;
                }
            }
            return result;
        }
        return {};
    }

private:
    inline bool finished() { return stable_done && delta_done; }

    template <bool c_stable_done, bool c_delta_done>
    inline void next(MutableColumns & output_columns, size_t & output_write_limit)
    {
        if constexpr (!c_stable_done)
        {
            if (use_stable_rows)
            {
                writeFromStable<c_delta_done>(output_columns, output_write_limit);
                return;
            }
        }

        if constexpr (!c_delta_done)
        {
            if (use_delta_rows)
            {
                writeInsertFromDelta(output_columns, output_write_limit);
            }
            else
            {
                auto cur_index = index[index_pos];
                switch (cur_index.type)
                {
                case DT_DEL:
                    writeDeleteFromDelta(cur_index.count);
                    break;
                case DT_INS:
                {
                    use_delta_offset = cur_index.value;
                    use_delta_rows   = cur_index.count;
                    writeInsertFromDelta(output_columns, output_write_limit);
                    break;
                }
                default:
                    throw Exception("Entry type " + DTTypeString(cur_index.type) + " is not supported, is end: "
                                    + DB::toString(index_pos == index_valid_size) + ", use_stable_rows: " + DB::toString(use_stable_rows)
                                    + ", stable_skip: " + DB::toString(stable_skip) + ", stable_done: " + DB::toString(stable_done)
                                    + ", delta_done: " + DB::toString(delta_done) + ", delta_done: " + DB::toString(delta_done));
                }
            }

            if (!use_delta_rows)
                stepForwardEntryIt();
        }
    }

    inline void initOutputColumns(MutableColumns & columns)
    {
        columns.resize(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            columns[i] = header.safeGetByPosition(i).column->cloneEmpty();
        }
    }

    inline size_t curStableBlockRemaining() { return cur_stable_block_rows - cur_stable_block_pos; }

    inline size_t skipRowsInCurStableBlock(size_t n)
    {
        size_t remaining = cur_stable_block_rows - cur_stable_block_pos;
        size_t skipped   = std::min(n, remaining);
        cur_stable_block_pos += skipped;
        return skipped;
    }

    inline bool fillStableBlockIfNeeded()
    {
        if (!cur_stable_block_columns.empty() && cur_stable_block_pos < cur_stable_block_rows)
            return true;

        cur_stable_block_columns.clear();
        cur_stable_block_rows = 0;
        cur_stable_block_pos  = 0;
        auto block            = stable_input_stream->read();
        if (!block || !block.rows())
            return false;

        cur_stable_block_rows = block.rows();
        for (size_t column_id = 0; column_id < num_columns; ++column_id)
            cur_stable_block_columns.push_back(block.getByPosition(column_id).column);
        return true;
    }

    template <bool c_delta_done>
    void writeFromStable(MutableColumns & output_columns, size_t & output_write_limit)
    {
        // First let's do skip caused by stable_skip.
        while (stable_skip > 0)
        {
            if (curStableBlockRemaining())
            {
                stable_skip -= skipRowsInCurStableBlock(stable_skip);
                continue;
            }

            size_t stable_skipped_rows;
            stable_input_stream->getSkippedRows(stable_skipped_rows);

            if (stable_skipped_rows > 0)
            {
                stable_skip -= stable_skipped_rows;
                continue;
            }
            else
            {
                if (!fillStableBlockIfNeeded())
                    throw Exception("Unexpected end of stable stream, need more rows to skip");
            }
        }

        if (stable_skip < 0)
        {
            if (use_stable_rows > (size_t)(std::abs(stable_skip)))
            {
                use_stable_rows += stable_skip;
                stable_skip = 0;
            }
            else
            {
                stable_skip += use_stable_rows;
                // Nothing to write, because those rows we want to write are skipped already.
                use_stable_rows = 0;
            }
        }

        if (unlikely(!(stable_skip == 0 || use_stable_rows == 0)))
            throw Exception("Algorithm broken!");

        while (use_stable_rows && output_write_limit)
        {
            if (curStableBlockRemaining())
            {
                writeCurStableBlock(output_columns, output_write_limit);
                continue;
            }

            size_t stable_skipped_rows;
            stable_input_stream->getSkippedRows(stable_skipped_rows);

            if (stable_skipped_rows > 0)
            {
                if (stable_skipped_rows <= use_stable_rows)
                {
                    use_stable_rows -= stable_skipped_rows;
                }
                else
                {
                    stable_skip -= stable_skipped_rows - use_stable_rows;
                    use_stable_rows = 0;
                }
            }
            else
            {
                if (!fillStableBlockIfNeeded())
                {
                    if constexpr (c_delta_done)
                    {
                        stable_done     = true;
                        use_stable_rows = 0;
                        break;
                    }
                    else
                        throw Exception("Unexpected end of stable stream, need more rows to write");
                }
            }
        }
    }

    // Return number of rows written into output
    inline void writeCurStableBlock(MutableColumns & output_columns, size_t & output_write_limit)
    {
        auto output_offset = output_columns.at(0)->size();

        size_t copy_rows  = std::min(output_write_limit, use_stable_rows);
        copy_rows         = std::min(copy_rows, cur_stable_block_rows - cur_stable_block_pos);
        auto valid_offset = cur_stable_block_pos;
        auto valid_limit  = copy_rows;

        std::tie(valid_offset, valid_limit)
            = HandleFilter::getPosRangeOfSorted(handle_range, cur_stable_block_columns[0], valid_offset, valid_limit);

        if (!output_offset && !valid_offset && valid_limit == cur_stable_block_rows)
        {
            // Simply return columns in current stable block.
            for (size_t column_id = 0; column_id < output_columns.size(); ++column_id)
                output_columns[column_id] = (*std::move(cur_stable_block_columns[column_id])).mutate();

            // Let's return current stable block directly. No more expending.
            output_write_limit = 0;
        }
        else if (valid_limit)
        {
            // Prevent frequently reallocation.
            if (output_columns[0]->empty())
            {
                for (size_t column_id = 0; column_id < num_columns; ++column_id)
                    output_columns[column_id]->reserve(max_block_size);
            }
            for (size_t column_id = 0; column_id < num_columns; ++column_id)
                output_columns[column_id]->insertRangeFrom(*cur_stable_block_columns[column_id], valid_offset, valid_limit);

            output_write_limit -= std::min(valid_limit, output_write_limit);
        }

        cur_stable_block_pos += copy_rows;
        use_stable_rows -= copy_rows;
    }

    inline void writeInsertFromDelta(MutableColumns & output_columns, size_t & output_write_limit)
    {
        auto write_rows = std::min(output_write_limit, use_delta_rows);

        // Prevent frequently reallocation.
        if (output_columns[0]->empty())
        {
            for (size_t column_id = 0; column_id < num_columns; ++column_id)
                output_columns[column_id]->reserve(max_block_size);
        }

        auto actually_write_rows = delta_value_space->write(output_columns, use_delta_offset, write_rows);

        output_write_limit -= actually_write_rows;
        use_delta_offset += write_rows;
        use_delta_rows -= write_rows;
    }

    inline void writeDeleteFromDelta(size_t n) { stable_skip += n; }

    inline void stepForwardEntryIt()
    {
        UInt64 prev_sid;
        {
            auto cur_index = index[index_pos];
            prev_sid       = cur_index.sid;
            if (cur_index.type == DT_DEL)
                prev_sid += cur_index.count;
        }

        ++index_pos;

        if (index_pos == index_valid_size)
        {
            delta_done      = true;
            use_stable_rows = UNLIMITED;
        }
        else
        {
            use_stable_rows = index[index_pos].sid - prev_sid;
        }
    }
}; // namespace DM

} // namespace DM
} // namespace DB