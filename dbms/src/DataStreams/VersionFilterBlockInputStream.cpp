#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/VersionFilterBlockInputStream.h>
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

Block VersionFilterBlockInputStream::readImpl()
{
    while (true)
    {
        Block block = input->read();
        if (!block)
            return block;

        const ColumnWithTypeAndName & version_column = block.getByPosition(version_column_index);
        const ColumnUInt64 * column = static_cast<const ColumnUInt64 *>(version_column.column.get());

        size_t rows = block.rows();

        const UInt64 * data_start = column->getData().data();
        const UInt64 * data_end = data_start + rows;
        const UInt64 * filter_start = nullptr;

        {
            alignas(SIMD_BYTES) std::array<UInt8, STEP> step_data{};
            std::ignore = step_data;
            const UInt64 * data_pos = data_start;

#if __SSE2__
            const UInt64 * data_end_sse = data_start + rows / STEP * STEP;
            for (; data_pos != data_end_sse; data_pos += STEP)
            {
                for (int i = 0; i < STEP; ++i)
                    step_data[i] = data_pos[i] > filter_greater_version;
                int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(step_data.data())), zero16));
                if (mask)
                {
                    filter_start = data_pos;
                    break;
                }
            }
#endif

            if (filter_start == nullptr)
            {
                for (; data_pos != data_end; ++data_pos)
                {
                    if (data_pos[0] > filter_greater_version)
                    {
                        filter_start = data_pos;
                        break;
                    }
                }
            }
        }

        if (filter_start == nullptr)
            return block;

        IColumn::Filter col_filter(rows, 1);

        {
            UInt8 * filter_pos = col_filter.data() + (filter_start - data_start);
            const UInt64 * data_pos = filter_start;
            for (; data_pos != data_end; ++data_pos, ++filter_pos)
                filter_pos[0] = !(data_pos[0] > filter_greater_version);
        }

        if (filter_start == data_start && memoryIsZero(col_filter.data(), rows))
            continue;

        return filterBlock(block, col_filter);
    }
}

} // namespace DB
