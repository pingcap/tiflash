#pragma once

#include <Common/Exception.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/HandleFilter.h>
#include "ChunkBlockInputStream.h"

namespace DB
{
namespace DM
{

// Note that the columns in stable input stream and value space must exactly the same, including name, type, and id.
template <class DeltaValueSpace, class IndexIterator>
class DeltaMergeBlockInputStream final : public IProfilingBlockInputStream
{
    static constexpr size_t UNLIMITED = std::numeric_limits<UInt64>::max();

private:
    using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;
    using SharedLock         = std::shared_lock<std::shared_mutex>;

    ChunkBlockInputStreamPtr stable_input_stream;
    ChunkBlockInputStream *  stable_input_stream_raw_ptr;

    // How many rows we need to skip before writing stable rows into output.
    // == 0: None
    // > 0 : do skip
    // < 0 : some rows are filtered out by index, should not write into output.
    ssize_t stable_skip = 0;

    DeltaValueSpacePtr delta_value_space;
    IndexIterator      entry_it;
    IndexIterator      entry_end;

    size_t max_block_size;

    Block  header;
    size_t num_columns;

    size_t use_stable_rows;

    Columns cur_stable_block_columns;
    size_t  cur_stable_block_rows = 0;
    size_t  cur_stable_block_pos  = 0;

    bool stable_done = false;
    bool delta_done  = false;

public:
    DeltaMergeBlockInputStream(const ChunkBlockInputStreamPtr & stable_input_stream_,
                               const DeltaValueSpacePtr &       delta_value_space_,
                               IndexIterator                    index_begin,
                               IndexIterator                    index_end,
                               size_t                           max_block_size_)
        : stable_input_stream(stable_input_stream_),
          stable_input_stream_raw_ptr(stable_input_stream.get()),
          delta_value_space(delta_value_space_),
          entry_it(index_begin),
          entry_end(index_end),
          max_block_size(max_block_size_)
    {
        header      = stable_input_stream_raw_ptr->getHeader();
        num_columns = header.columns();

        if (entry_it == entry_end)
        {
            use_stable_rows = UNLIMITED;
            delta_done      = true;
        }
        else
        {
            use_stable_rows = entry_it.getSid();
        }
    }

    String getName() const override { return "DeltaMerge"; }
    Block  getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
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

            return header.cloneWithColumns(std::move(columns));
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
            auto tuple_id = entry_it.getValue();
            switch (entry_it.getType())
            {
            case DT_DEL:
                writeDeleteFromDelta(1);
                break;
            case DT_INS:
                writeInsertFromDelta(output_columns, tuple_id);
                --output_write_limit;
                break;
            default:
                throw Exception("Entry type " + DTTypeString(entry_it.getType()) + " is not supported, is end: "
                                + DB::toString(entry_it == entry_end) + ", use_stable_rows: " + DB::toString(use_stable_rows)
                                + ", stable_skip: " + DB::toString(stable_skip) + ", stable_done: " + DB::toString(stable_done)
                                + ", delta_done: " + DB::toString(delta_done) + ", delta_done: " + DB::toString(delta_done));
            }

            stepForwardEntryIt();
        }
    }

    inline void initOutputColumns(MutableColumns & columns)
    {
        columns.resize(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            columns[i] = header.safeGetByPosition(i).column->cloneEmpty();
            columns[i]->reserve(max_block_size);
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

    // Return number of rows written into output
    inline void writeCurStableBlock(MutableColumns & output_columns, size_t & output_write_limit)
    {
        auto output_offset = output_columns.at(0)->size();

        size_t copy_rows  = std::min(output_write_limit, use_stable_rows);
        copy_rows         = std::min(copy_rows, cur_stable_block_rows - cur_stable_block_pos);
        auto valid_offset = cur_stable_block_pos;
        auto valid_limit  = copy_rows;

        if (!output_offset && !valid_offset && valid_limit == cur_stable_block_rows)
        {
            // Simply return columns in current stable block.
            for (size_t column_id = 0; column_id < output_columns.size(); ++column_id)
                output_columns[column_id] = (*std::move(cur_stable_block_columns[column_id])).mutate();
        }
        else if (valid_limit)
        {
            for (size_t column_id = 0; column_id < num_columns; ++column_id)
                output_columns[column_id]->insertRangeFrom(*cur_stable_block_columns[column_id], valid_offset, valid_limit);
        }

        cur_stable_block_pos += copy_rows;
        use_stable_rows -= copy_rows;
        output_write_limit -= copy_rows;
    }

    inline bool fillStableBlockIfNeeded()
    {
        if (!cur_stable_block_columns.empty() && cur_stable_block_pos < cur_stable_block_rows)
            return true;

        cur_stable_block_columns.clear();
        cur_stable_block_rows = 0;
        cur_stable_block_pos  = 0;
        auto block            = stable_input_stream_raw_ptr->read();
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
            // Check whether we can skip next block entirely or not.
            if (!stable_input_stream_raw_ptr->hasNext())
                throw Exception("Unexpected end of block, need more rows to skip");

            size_t rows = stable_input_stream_raw_ptr->nextRows();
            if (!stable_input_stream_raw_ptr->shouldSkipNext())
            {
                fillStableBlockIfNeeded();
            }
            else
            {
                stable_input_stream_raw_ptr->skipNext();
                stable_skip -= rows;
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

            if (!stable_input_stream_raw_ptr->hasNext())
            {
                if constexpr (c_delta_done)
                {
                    stable_done     = true;
                    use_stable_rows = 0;
                    break;
                }
                else
                    throw Exception("Unexpected end of block, need more rows to write");
            }

            size_t next_block_rows = stable_input_stream_raw_ptr->nextRows();

            if (!stable_input_stream_raw_ptr->shouldSkipNext())
            {
                fillStableBlockIfNeeded();
            }
            else
            {
                // Entirely skip block.
                stable_input_stream_raw_ptr->skipNext();
                // We skipped some rows, some of them are consumed by writing to output, the rest are recorded by stable_skip.
                if (next_block_rows <= use_stable_rows)
                {
                    use_stable_rows -= next_block_rows;
                }
                else
                {
                    stable_skip -= next_block_rows - use_stable_rows;
                    use_stable_rows = 0;
                }
            }
        }
    }

    inline void writeInsertFromDelta(MutableColumns & output_columns, UInt64 tuple_id)
    {
        for (size_t index = 0; index < num_columns; ++index)
            delta_value_space->insertValue(*output_columns[index], index, tuple_id);
    }

    inline void writeDeleteFromDelta(size_t n) { stable_skip += n; }

    inline void stepForwardEntryIt()
    {
        auto prev_sid = entry_it.getSid();
        if (entry_it.getType() == DT_DEL)
            prev_sid += 1;

        ++entry_it;

        if (entry_it == entry_end)
        {
            delta_done      = true;
            use_stable_rows = UNLIMITED;
        }
        else
        {
            use_stable_rows = entry_it.getSid() - prev_sid;
        }
    }
}; // namespace DM

} // namespace DM
} // namespace DB