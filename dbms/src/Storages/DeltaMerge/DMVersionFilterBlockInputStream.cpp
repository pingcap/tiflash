#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>

namespace DB
{

namespace DM
{

static constexpr size_t UNROLL_BATCH = 64;

template <int MODE>
Block DMVersionFilterBlockInputStream<MODE>::readImpl()
{
    while (true)
    {
        if (!raw_block)
        {
            if (!initNextBlock())
                return {};
        }

        Block  cur_raw_block = raw_block;
        size_t rows          = cur_raw_block.rows();
        filter.resize(rows);
        size_t i = 0;

        i = (rows - 1) / UNROLL_BATCH * UNROLL_BATCH;

        if constexpr (MODE == DM_VESION_FILTER_MODE_MVCC)
        {
            for (size_t n = 0; n < i; n += UNROLL_BATCH)
            {
                for (size_t k = 0; k < UNROLL_BATCH; ++k)
                    filter[n + k] = (*version_col_data)[n + k + 1] > version_limit;
            }

            for (size_t n = 0; n < i; n += UNROLL_BATCH)
            {
                for (size_t k = 0; k < UNROLL_BATCH; ++k)
                    filter[n + k] |= (*handle_col_data)[n + k] != (*handle_col_data)[n + k + 1];
            }

            for (size_t n = 0; n < i; n += UNROLL_BATCH)
            {
                for (size_t k = 0; k < UNROLL_BATCH; ++k)
                    filter[n + k] &= (*version_col_data)[n + k] <= version_limit;
            }

            for (size_t n = 0; n < i; n += UNROLL_BATCH)
            {
                for (size_t k = 0; k < UNROLL_BATCH; ++k)
                    filter[n + k] &= !(*delete_col_data)[n + k];
            }
        }
        else if (MODE == DM_VESION_FILTER_MODE_COMPACT)
        {

            for (size_t n = 0; n < i; n += UNROLL_BATCH)
            {
                for (size_t k = 0; k < UNROLL_BATCH; ++k)
                {
                    filter[n + k] = (*handle_col_data)[n + k] != (*handle_col_data)[n + k + 1];
                }
            }
            for (size_t n = 0; n < i; n += UNROLL_BATCH)
            {
                for (size_t k = 0; k < UNROLL_BATCH; ++k)
                {
                    filter[n + k] &= !(*delete_col_data)[n + k];
                }
            }
            for (size_t n = 0; n < i; n += UNROLL_BATCH)
            {
                for (size_t k = 0; k < UNROLL_BATCH; ++k)
                {
                    filter[n + k] |= (*version_col_data)[n] >= version_limit;
                }
            }
        }
        else
        {
            throw Exception("Unsupported mode");
        }


        for (; i < rows - 1; ++i)
            filter[i] = checkWithNextIndex(i);

        {
            // Now let's handle the last row of current block.
            auto cur_handle  = (*handle_col_data)[rows - 1];
            auto cur_version = (*version_col_data)[rows - 1];
            auto deleted     = (*delete_col_data)[rows - 1];
            if (!initNextBlock())
            {
                // No more block.
                bool ok;
                if constexpr (MODE == DM_VESION_FILTER_MODE_MVCC)
                {
                    ok = !deleted && cur_version <= version_limit;
                }
                else if (MODE == DM_VESION_FILTER_MODE_COMPACT)
                {
                    ok = cur_version >= version_limit || !deleted;
                }
                else
                {
                    throw Exception("Unsupported mode");
                }

                filter[rows - 1] = ok;
            }
            else
            {
                auto next_handle  = (*handle_col_data)[0];
                auto next_version = (*version_col_data)[0];
                bool ok;
                if constexpr (MODE == DM_VESION_FILTER_MODE_MVCC)
                {
                    ok = !deleted && cur_version <= version_limit && (cur_handle != next_handle || next_version > version_limit);
                }
                else if (MODE == DM_VESION_FILTER_MODE_COMPACT)
                {
                    ok = cur_version >= version_limit || (cur_handle != next_handle && !deleted);
                }
                else
                {
                    throw Exception("Unsupported mode");
                }
                filter[rows - 1] = ok;
            }
        }

        size_t passed_count = countBytesInFilter(filter);

        if (!passed_count)
            continue;
        if (passed_count == rows)
            return cur_raw_block;

        for (size_t col_index = 0; col_index < cur_raw_block.columns(); ++col_index)
        {
            auto & column = cur_raw_block.getByPosition(col_index);
            column.column = column.column->filter(filter, passed_count);
        }
        return cur_raw_block;
    }
}

template class DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_MVCC>;
template class DMVersionFilterBlockInputStream<DM_VESION_FILTER_MODE_COMPACT>;

} // namespace DM
} // namespace DB