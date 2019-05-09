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

static inline bool isAllZero(const UInt8 * data, const size_t size)
{
    const UInt8 * data_pos = data;
    const UInt8 * data_end = data_pos + size;

#if __SSE2__
    const UInt8 * data_end_sse = data_pos + size / SIMD_BYTES * SIMD_BYTES;

    for (; data_pos != data_end_sse; data_pos += SIMD_BYTES)
    {
        int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(data_pos)), zero16));

        if (mask)
            return false;
    }
#endif

    for (; data_pos != data_end; ++data_pos)
    {
        if (data_pos[0])
            return false;
    }

    return true;
}

Block VersionFilterBlockInputStream::readImpl()
{
    while (true)
    {
        Block block = input->read();
        if (!block)
            return block;

        if (!block.has(MutableSupport::version_column_name))
        {
            throw Exception(
                "VersionFilterBlockInputStream: block without " + MutableSupport::version_column_name, ErrorCodes::LOGICAL_ERROR);
        }

        const ColumnWithTypeAndName & version_column = block.getByName(version_column_name);
        const ColumnUInt64 * column = static_cast<const ColumnUInt64 *>(version_column.column.get());

        size_t rows = block.rows();

        const UInt64 * data_start = column->getData().data();
        const UInt64 * data_end = data_start + rows;

        constexpr size_t FILTER_START_NONE = std::numeric_limits<size_t>::max();
        size_t filter_start = FILTER_START_NONE;

        {
            std::array<UInt8, STEP> step_data{};
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
                    filter_start = data_pos - data_start;
                    break;
                }
            }
#endif

            if (filter_start == FILTER_START_NONE)
            {
                for (; data_pos != data_end; ++data_pos)
                {
                    if (data_pos[0] > filter_greater_version)
                    {
                        filter_start = data_pos - data_start;
                        break;
                    }
                }
            }
        }

        if (filter_start == FILTER_START_NONE)
            return block;

        IColumn::Filter col_filter(rows, 1);

        {
            UInt8 * filter_ptr = col_filter.data();
            for (size_t pos = filter_start; pos < rows; ++pos)
                filter_ptr[pos] = !(data_start[pos] > filter_greater_version);
        }

        if (filter_start == 0 && isAllZero(col_filter.data(), rows))
            continue;

        deleteRows(block, col_filter);
        return block;
    }
}

} // namespace DB
