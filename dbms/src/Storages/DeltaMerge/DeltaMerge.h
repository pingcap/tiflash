// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <DataStreams/IBlockInputStream.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace ProfileEvents
{
extern const Event DTDeltaIndexError;
} // namespace ProfileEvents


namespace DB::ErrorCodes
{
extern const int DT_DELTA_INDEX_ERROR;
}

namespace DB::DM
{
/// Note that the columns in stable input stream and value space must exactly the same, including name, type, and id.
/// The first column must be PK column.
/// This class does not guarantee that the rows in the return blocks are filltered by range.
template <bool skippable_place = false, bool need_row_id = false>
class DeltaMergeBlockInputStream final
    : public SkippableBlockInputStream
    , Allocator<false>
{
    static constexpr size_t UNLIMITED = std::numeric_limits<UInt64>::max();

private:
    using DeltaValueReaderPtr = std::shared_ptr<DeltaValueReader>;

    SkippableBlockInputStreamPtr stable_input_stream;

    // How many rows we need to ignore before writing stable rows into output.
    // == 0: None
    // > 0 : some rows are ignored by delta tree (deletes), should not write into output.
    // < 0 : some rows are filtered out by stable filter, should not write into output.
    ssize_t stable_ignore = 0;

    /// Those vars are only used in skippable_place mode
    size_t sk_call_status = 0; // 0: initial, 1: called once by getSkippedRows
    size_t sk_skip_stable_rows = 0;
    size_t sk_skip_total_rows = 0;
    Block sk_first_block;

    DeltaValueReaderPtr delta_value_reader;
    DeltaIndexIterator delta_index_it;
    DeltaIndexIterator delta_index_end;

    RowKeyRange rowkey_range;
    bool is_common_handle;
    size_t rowkey_column_size;

    size_t max_block_size;

    Block header;
    size_t num_columns;

    size_t use_stable_rows;

    size_t use_delta_offset = 0;
    size_t use_delta_rows = 0;

    Columns cur_stable_block_columns;
    size_t cur_stable_block_rows = 0;
    size_t cur_stable_block_pos = 0;
    UInt64 cur_stable_block_start_offset = 0;

    bool stable_done = false;
    bool delta_done = false;

    // How many times `read` is called.
    size_t num_read = 0;

    RowKeyValue last_value;
    RowKeyValueRef last_value_ref{};
    UInt64 last_version = 0;
    size_t last_handle_pos = 0;
    size_t last_handle_read_num = 0;

    // Use for calculating MVCC-bitmap-filter when `need_row_id` is true.
    ColumnUInt32::MutablePtr seg_row_id_col;
    // `stable_rows` is the total rows of the underlying DMFiles, includes not valid rows.
    UInt64 stable_rows;
    // `delta_row_ids` is used to return the row id of delta.
    std::vector<UInt32> delta_row_ids;

    const String tracing_id;

public:
    DeltaMergeBlockInputStream(
        const SkippableBlockInputStreamPtr & stable_input_stream_,
        const DeltaValueReaderPtr & delta_value_reader_,
        const DeltaIndexIterator & delta_index_start_,
        const DeltaIndexIterator & delta_index_end_,
        const RowKeyRange rowkey_range_,
        size_t max_block_size_,
        UInt64 stable_rows_,
        const String & tracing_id_)
        : stable_input_stream(stable_input_stream_)
        , delta_value_reader(delta_value_reader_)
        , delta_index_it(delta_index_start_)
        , delta_index_end(delta_index_end_)
        , rowkey_range(rowkey_range_)
        , is_common_handle(rowkey_range.is_common_handle)
        , rowkey_column_size(rowkey_range.rowkey_column_size)
        , max_block_size(max_block_size_)
        , stable_rows(stable_rows_)
        , tracing_id(tracing_id_)
    {
        if constexpr (skippable_place)
        {
            if (!rowkey_range.isEndInfinite())
                throw Exception("The end of rowkey range should be +Inf in skippable_place mode");
        }

        header = stable_input_stream->getHeader();
        num_columns = header.columns();

        if (delta_index_it == delta_index_end)
        {
            use_stable_rows = UNLIMITED;
            delta_done = true;
        }
        else
        {
            use_stable_rows = delta_index_it.getSid();
        }
        auto all_range = RowKeyRange::newAll(is_common_handle, rowkey_column_size);
        last_value = all_range.getStart().toRowKeyValue();
        last_value_ref = last_value.toRowKeyValueRef();
    }

    String getName() const override { return "DeltaMerge"; }
    Block getHeader() const override { return header; }

    bool getSkippedRows(size_t & skip_rows) override
    {
        if constexpr (!skippable_place)
        {
            skip_rows = 0;
            return true;
        }

        if (sk_call_status != 0)
            throw Exception("Call #getSkippedRows() more than once");
        ++sk_call_status;

        stable_input_stream->getSkippedRows(sk_skip_stable_rows);
        stable_ignore -= sk_skip_stable_rows;

        sk_first_block = doRead();

        skip_rows = sk_skip_total_rows;
        sk_skip_stable_rows = 0;
        sk_skip_total_rows = 0;
        return true;
    }

    size_t skipNextBlock() override { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }
    Block readWithFilter(const IColumn::Filter &) override
    {
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    Block read() override
    {
        if constexpr (skippable_place)
        {
            if (sk_call_status == 0)
                throw Exception("Unexpected call #read() in status 0");
            if (sk_call_status == 1 && sk_first_block)
            {
                Block tmp;
                tmp.swap(sk_first_block);
                beforeReturnBlock(tmp);
                ++sk_call_status;
                return tmp;
            }
        }

        auto block = doRead();
        if (block)
            beforeReturnBlock(block);
        return block;
    }

private:
    void beforeReturnBlock(const Block & block)
    {
        if constexpr (DM_RUN_CHECK)
        {
            // In some cases like Segment::getSplitPointSlow, only handle column in block.
            if (block.columns() < 2 //
                || block.getByPosition(0).column_id != MutSup::extra_handle_id //
                || block.getByPosition(1).column_id != MutSup::version_col_id)
                return;

            ++num_read;

            auto rowkey_column = RowKeyColumnContainer(block.getByPosition(0).column, is_common_handle);
            const auto & version_column = toColumnVectorData<UInt64>(block.getByPosition(1).column);
            for (size_t i = 0; i < version_column.size(); ++i)
            {
                auto rowkey_value = rowkey_column.getRowKeyValue(i);
                auto version = version_column[i];
                auto cmp_result = rowkey_value <=> last_value_ref;
                if (cmp_result == std::strong_ordering::less
                    || (cmp_result == std::strong_ordering::equal && version < last_version))
                {
                    ProfileEvents::increment(ProfileEvents::DTDeltaIndexError);
                    LOG_ERROR(
                        Logger::get(tracing_id),
                        "DeltaMerge return wrong result, current handle[{}]version[{}]@read[{}]@pos[{}] "
                        "is expected >= last_handle[{}]last_version[{}]@read[{}]@pos[{}]",
                        rowkey_value.toDebugString(),
                        version,
                        num_read,
                        i,
                        last_value_ref.toDebugString(),
                        last_version,
                        last_handle_read_num,
                        last_handle_pos);

                    throw Exception(
                        ErrorCodes::DT_DELTA_INDEX_ERROR,
                        "DeltaMerge return wrong result, current handle[{}]version[{}]@read[{}]@pos[{}] "
                        "is expected >= last_handle[{}]last_version[{}]@read[{}]@pos[{}]",
                        rowkey_value.toDebugString(),
                        version,
                        num_read,
                        i,
                        last_value_ref.toDebugString(),
                        last_version,
                        last_handle_read_num,
                        last_handle_pos);
                }
                last_value_ref = rowkey_value;
                last_version = version;
                last_handle_pos = i;
                last_handle_read_num = num_read;
            }
            /// last_value is based on block, when block is released, it will
            /// become invalid, so need to update last_value here
            last_value = last_value_ref.toRowKeyValue();
            last_value_ref = last_value.toRowKeyValueRef();

            if constexpr (need_row_id)
            {
                RUNTIME_CHECK_MSG(
                    block.rows() == block.segmentRowIdCol()->size(),
                    "Build bitmap error: block.rows {} != segmentRowId.size() {}",
                    block.rows(),
                    block.segmentRowIdCol()->size());
            }
        }
    }

    Block doRead()
    {
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

            auto block = header.cloneWithColumns(std::move(columns));
            if constexpr (need_row_id)
            {
                block.setSegmentRowIdCol(std::move(seg_row_id_col));
            }
            return block;
        }
        return {};
    }

private:
    inline bool finished() { return stable_done && delta_done; }

    inline void fillSegmentRowId(UInt64 start, UInt64 limit)
    {
        auto & v = seg_row_id_col->getData();
        auto offset = v.size();
        v.resize(v.size() + limit);
        for (UInt64 i = 0; i < limit; ++i)
        {
            v[offset + i] = start + i;
        }
    }

    inline void fillSegmentRowId(const std::vector<UInt32> & row_ids)
    {
        auto & v = seg_row_id_col->getData();
        auto offset = v.size();
        v.resize(v.size() + row_ids.size());
        for (UInt32 i = 0; i < row_ids.size(); ++i)
        {
            v[offset + i] = row_ids[i] + stable_rows;
        }
    }
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
                if (delta_index_it.isDelete())
                {
                    // Delete.
                    writeDeleteFromDelta(delta_index_it.getCount());
                }
                else
                {
                    // Insert.
                    bool do_write = true;
                    if constexpr (skippable_place)
                    {
                        if (delta_index_it.getSid() < sk_skip_stable_rows)
                        {
                            do_write = false;
                            sk_skip_total_rows += delta_index_it.getCount();
                        }
                    }

                    if (do_write)
                    {
                        use_delta_offset = delta_index_it.getValue();
                        use_delta_rows = delta_index_it.getCount();
                        writeInsertFromDelta(output_columns, output_write_limit);
                    }
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
        if constexpr (need_row_id)
        {
            seg_row_id_col = ColumnUInt32::create();
            seg_row_id_col->reserve(max_block_size);
        }
    }

    inline size_t curStableBlockRemaining() { return cur_stable_block_rows - cur_stable_block_pos; }

    inline size_t skipRowsInCurStableBlock(size_t n)
    {
        size_t remaining = cur_stable_block_rows - cur_stable_block_pos;
        size_t skipped = std::min(n, remaining);
        cur_stable_block_pos += skipped;
        return skipped;
    }

    inline bool fillStableBlockIfNeeded()
    {
        if (!cur_stable_block_columns.empty() && cur_stable_block_pos < cur_stable_block_rows)
            return true;

        cur_stable_block_columns.clear();
        cur_stable_block_rows = 0;
        cur_stable_block_pos = 0;
        cur_stable_block_start_offset = 0;
        auto block = stable_input_stream->read();
        if (!block || !block.rows())
            return false;

        cur_stable_block_rows = block.rows();
        cur_stable_block_start_offset = block.startOffset();
        for (size_t column_id = 0; column_id < num_columns; ++column_id)
            cur_stable_block_columns.push_back(block.getByPosition(column_id).column);
        return true;
    }

    template <bool c_delta_done>
    void writeFromStable(MutableColumns & output_columns, size_t & output_write_limit)
    {
        // First let's do skip caused by stable_ignore.
        while (stable_ignore > 0)
        {
            if (curStableBlockRemaining())
            {
                stable_ignore -= skipRowsInCurStableBlock(stable_ignore);
                continue;
            }

            size_t skips;
            stable_input_stream->getSkippedRows(skips);

            if (skips > 0)
            {
                stable_ignore -= skips;
                continue;
            }
            else
            {
                if (!fillStableBlockIfNeeded())
                    throw Exception("Unexpected end of stable stream, need more rows to skip");
            }
        }

        if (stable_ignore < 0)
        {
            auto stable_ignore_abs = static_cast<size_t>(std::abs(stable_ignore));
            if (use_stable_rows > stable_ignore_abs)
            {
                use_stable_rows += stable_ignore;
                if constexpr (skippable_place)
                    sk_skip_total_rows += stable_ignore_abs;
                stable_ignore = 0;
            }
            else
            {
                stable_ignore += use_stable_rows;
                if constexpr (skippable_place)
                    sk_skip_total_rows += use_stable_rows;
                // Nothing to write, because those rows we want to write are skipped already.
                use_stable_rows = 0;
            }
        }

        if (unlikely(!(stable_ignore == 0 || use_stable_rows == 0)))
            throw Exception("Algorithm broken!");

        while (use_stable_rows && output_write_limit)
        {
            if (curStableBlockRemaining())
            {
                writeCurStableBlock(output_columns, output_write_limit);
                continue;
            }

            size_t skips;
            stable_input_stream->getSkippedRows(skips);

            if (skips > 0)
            {
                if (skips <= use_stable_rows)
                {
                    use_stable_rows -= skips;
                }
                else
                {
                    stable_ignore -= skips - use_stable_rows;
                    use_stable_rows = 0;
                }
            }
            else
            {
                if (!fillStableBlockIfNeeded())
                {
                    if constexpr (c_delta_done)
                    {
                        stable_done = true;
                        use_stable_rows = 0;
                        break;
                    }
                    else
                        throw Exception("Unexpected end of stable stream, need more rows to write");
                }
            }
        }
    }

    inline void writeCurStableBlock(MutableColumns & output_columns, size_t & output_write_limit)
    {
        auto output_offset = output_columns.at(0)->size();

        size_t copy_rows = std::min(output_write_limit, use_stable_rows);
        copy_rows = std::min(copy_rows, cur_stable_block_rows - cur_stable_block_pos);
        auto offset = cur_stable_block_pos;
        auto limit = copy_rows;

        // To get the final offset and limit of the rows within the range.
        // Note: Actually, it is not necessary, since handle column RoughSetFilter can filter out the packs out of range,
        // and the DMRowKeyFilterBlockInputStream can filter out the rest rows out of range.
        // TODO: Remove this.
        auto [final_offset, final_limit]
            = RowKeyFilter::getPosRangeOfSorted(rowkey_range, cur_stable_block_columns[0], offset, limit);

        if constexpr (skippable_place)
        {
            sk_skip_total_rows += final_offset - offset;
        }

        if (!output_offset && !final_offset && final_limit == cur_stable_block_rows)
        {
            // Simply return columns in current stable block.
            for (size_t column_id = 0; column_id < output_columns.size(); ++column_id)
                output_columns[column_id] = (*std::move(cur_stable_block_columns[column_id])).mutate();

            // Let's return current stable block directly. No more expending.
            output_write_limit = 0;
        }
        else if (final_limit)
        {
            // Prevent frequently reallocation.
            if (output_columns[0]->empty())
            {
                for (size_t column_id = 0; column_id < num_columns; ++column_id)
                    output_columns[column_id]->reserve(max_block_size);
            }
            for (size_t column_id = 0; column_id < num_columns; ++column_id)
                output_columns[column_id]->insertRangeFrom(
                    *cur_stable_block_columns[column_id],
                    final_offset,
                    final_limit);

            output_write_limit -= std::min(final_limit, output_write_limit);
        }

        if constexpr (need_row_id)
        {
            fillSegmentRowId(final_offset + cur_stable_block_start_offset, final_limit);
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

        // Note that the rows between [use_delta_offset, use_delta_offset + write_rows) are guaranteed sorted,
        // otherwise we won't read them in the same range.
        size_t actual_write = 0;
        if constexpr (need_row_id)
        {
            delta_row_ids.clear();
            delta_row_ids.reserve(write_rows);
            actual_write = delta_value_reader
                               ->readRows(output_columns, use_delta_offset, write_rows, &rowkey_range, &delta_row_ids);
            fillSegmentRowId(delta_row_ids);
        }
        else
        {
            actual_write = delta_value_reader->readRows(output_columns, use_delta_offset, write_rows, &rowkey_range);
        }

        if constexpr (skippable_place)
        {
            sk_skip_total_rows += write_rows - actual_write;
        }

        output_write_limit -= actual_write;
        use_delta_offset += write_rows;
        use_delta_rows -= write_rows;
    }

    inline void writeDeleteFromDelta(size_t n) { stable_ignore += n; }

    inline void stepForwardEntryIt()
    {
        UInt64 prev_sid;
        {
            prev_sid = delta_index_it.getSid();
            if (delta_index_it.isDelete())
                prev_sid += delta_index_it.getCount();
        }

        ++delta_index_it;

        if (delta_index_it == delta_index_end)
        {
            delta_done = true;
            use_stable_rows = UNLIMITED;
        }
        else
        {
            use_stable_rows = delta_index_it.getSid() - prev_sid;
        }
    }
};

} // namespace DB::DM
