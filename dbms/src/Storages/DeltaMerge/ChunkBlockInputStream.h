#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/HandleFilter.h>

namespace DB
{
namespace DM
{
/// Read `chunks` as blocks.
/// We can use `handle_range` param to filter out rows, and use `filter` to ignore some chunks roughly.
///
/// Note that `handle_range` param assumes that data in chunks are in order of handle. If not, please use handle range of {MIN, MAX}.
///
/// For example:
///     size_t skip_rows = 0;
///     while(stream.hasNext())
///     {
///         if(stream.shouldSkipNext())
///         {
///             skip_rows += stream.nextRows();
///             stream.skipNext();
///             continue;
///         }
///         auto block = stream.read();
///         ...
///     }
class ChunkBlockInputStream final : public IBlockInputStream
{
public:
    ChunkBlockInputStream(const Chunks &        chunks_,
                          size_t                handle_col_pos_,
                          const HandleRange &   handle_range_,
                          const ColumnDefines & read_columns_,
                          const PageReader &    page_reader_,
                          const RSOperatorPtr & filter_)
        : chunks(chunks_),
          handle_col_pos(handle_col_pos_),
          handle_range(handle_range_),
          read_columns(read_columns_),
          page_reader(page_reader_),
          filter(filter_)
    {
    }

    String getName() const override { return "Chunk"; }
    Block  getHeader() const override { return toEmptyBlock(read_columns); }

    Block read() override
    {
        if (!hasNext())
            return {};
        Block tmp;
        if (!cur_chunk_data)
            // It means user ignore the skipNext() result and insist to read data.
            tmp = readCurChunkData();
        else
            tmp.swap(cur_chunk_data);

        ++cur_chunk_index;
        cur_chunk_skip = false;

        return tmp;
    }

    bool hasNext()
    {
        if (cur_chunk_index >= chunks.size())
            return false;
        // Filter out those rows not fit for handle_range.
        for (; cur_chunk_index < chunks.size(); ++cur_chunk_index)
        {
            auto [first, last] = chunks[cur_chunk_index].getHandleFirstLast();
            if (handle_range.intersect(first, last))
                break;
        }

        if (cur_chunk_index >= chunks.size())
            return false;

        if (!cur_chunk_data)
        {
            if (filter)
            {
                auto &       chunk = chunks[cur_chunk_index];
                RSCheckParam param;
                for (auto & [col_id, meta] : chunk.getMetas())
                    param.indexes.emplace(col_id, RSIndex(meta.type, meta.minmax));

                cur_chunk_skip = filter->roughCheck(param) == None;
            }
            if (!cur_chunk_skip)
            {
                cur_chunk_data = readCurChunkData();
            }
        }

        return true;
    }

    size_t nextRows()
    {
        auto & chunk = chunks[cur_chunk_index];
        if (isCurChunkCompleted(chunk))
            return chunk.getRows();

        // Otherwise, some rows of current chunk are filtered out by handle_range.

        if (cur_chunk_data)
        {
            return cur_chunk_data.rows();
        }
        else
        {
            // Current chunk is ignored by `filter`,
            // but we still need to get the row count which their handles are included by handle_range.
            auto block = readChunk(chunk, {read_columns[handle_col_pos]}, page_reader);
            auto offset_limit
                = HandleFilter::getPosRangeOfSorted(handle_range, block.getByPosition(handle_col_pos).column, 0, block.rows());
            return offset_limit.second;
        }
    }

    bool shouldSkipNext() { return cur_chunk_skip; }

    void skipNext()
    {
        ++cur_chunk_index;

        cur_chunk_data = {};
        cur_chunk_skip = false;
    }

private:
    inline bool isCurChunkCompleted(const Chunk & chunk)
    {
        auto [first, last] = chunk.getHandleFirstLast();
        return handle_range.include(first, last);
    }

    inline Block readCurChunkData()
    {
        auto & chunk = chunks[cur_chunk_index];
        if (isCurChunkCompleted(chunk))
        {
            return readChunk(chunk, read_columns, page_reader);
        }
        else
        {
            auto block = readChunk(chunk, read_columns, page_reader);
            return HandleFilter::filterSorted(handle_range, std::move(block), handle_col_pos);
        }
    }

private:
    Chunks      chunks;
    size_t      handle_col_pos;
    HandleRange handle_range;

    ColumnDefines read_columns;
    PageReader    page_reader;
    RSOperatorPtr filter;

    size_t cur_chunk_index = 0;
    bool   cur_chunk_skip  = false;
    Block  cur_chunk_data;
};

using ChunkBlockInputStreamPtr = std::shared_ptr<ChunkBlockInputStream>;

} // namespace DM
} // namespace DB