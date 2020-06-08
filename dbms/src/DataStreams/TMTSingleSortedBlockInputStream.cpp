#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/TMTSingleSortedBlockInputStream.h>
#include <DataStreams/dedupUtils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

static constexpr size_t SIMD_BYTES = 16;
static constexpr Int32 STEP = SIMD_BYTES;
#if __SSE2__
static const __m128i zero16 = _mm_setzero_si128();
#endif

// pos in columns is made const in MergeTreeDataSelectExecutor::read.
static const size_t pk_column_index = 0;
static const size_t delmark_column_index = 2;

/// tol_size > 0
static bool isPKDiffEachInSortedColumn(const UInt64 * data_start, size_t tol_size)
{
    tol_size -= 1;
    const UInt64 * data_pos = data_start;
    const UInt64 * data_end = data_start + tol_size;
    const UInt64 * data_end_sse = data_start + tol_size / STEP * STEP;
    std::ignore = data_end_sse;

#if __SSE2__

    alignas(STEP) std::array<UInt8, STEP> step_data{};

    for (; data_pos != data_end_sse; data_pos += STEP)
    {
        for (size_t i = 0; i < STEP; ++i)
            step_data[i] = data_pos[i] == data_pos[i + 1];
        int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(step_data.data())), zero16));
        if (mask)
            return false;
    }

#endif

    for (; data_pos != data_end; ++data_pos)
    {
        if (data_pos[0] == data_pos[1])
            return false;
    }

    return true;
}

inline UInt64 getPkInBlockAt(const Block & block, size_t pos)
{
    return static_cast<const ColumnUInt64 *>(block.getByPosition(pk_column_index).column.get())->getData()[pos];
}

void TMTSingleSortedBlockInputStream::updateNextBlock()
{
    if (!next_block)
        finish = true;
    cur_block = std::move(next_block);
}

Block TMTSingleSortedBlockInputStream::readImpl()
{
    while (true)
    {
        if (finish)
            return Block();

        {
            Block tmp_block = input->read();
            if (first)
            {
                if (!tmp_block)
                    return tmp_block;
                first = false;
                cur_block = std::move(tmp_block);
                continue;
            }
            next_block = std::move(tmp_block);
        }

        // cur_block is never empty.
        const size_t rows = cur_block.rows();

        // if not exist, just make it diff from the last element.
        UInt64 next_block_first_pk = next_block ? getPkInBlockAt(next_block, 0) : getPkInBlockAt(cur_block, rows - 1) + 1;

        const UInt64 * pk_column
            = static_cast<const ColumnUInt64 *>(cur_block.getByPosition(pk_column_index).column.get())->getData().data();
        const UInt8 * delmark_column
            = static_cast<const ColumnUInt8 *>(cur_block.getByPosition(delmark_column_index).column.get())->getData().data();

        if (isPKDiffEachInSortedColumn(pk_column, rows) && pk_column[rows - 1] != next_block_first_pk && memoryIsZero(delmark_column, rows))
        {
            auto tmp_block = std::move(cur_block);
            updateNextBlock();
            return tmp_block;
        }

        IColumn::Filter filter(rows, 0);
        for (size_t pos = 1; pos < rows; ++pos)
        {
            if (pk_column[pos] != pk_column[pos - 1])
                filter[pos - 1] = delmark_column[pos - 1] ^ (UInt8)1;
        }
        if (next_block_first_pk != pk_column[rows - 1])
            filter[rows - 1] = delmark_column[rows - 1] ^ (UInt8)1;

        if (memoryIsZero(filter.data(), rows))
        {
            updateNextBlock();
            continue;
        }
        else
        {
            Block res = filterBlock(cur_block, filter);
            updateNextBlock();
            return res;
        }
    }
}

} // namespace DB
