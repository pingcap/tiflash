#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>

namespace ProfileEvents
{
extern const Event DMCleanReadRows;
} // namespace ProfileEvents

namespace DB
{
namespace DM
{
template <int MODE>
void DMVersionFilterBlockInputStream<MODE>::readPrefix()
{
    forEachChild([](IBlockInputStream & child) {
        child.readPrefix();
        return false;
    });
}

template <int MODE>
void DMVersionFilterBlockInputStream<MODE>::readSuffix()
{
    forEachChild([](IBlockInputStream & child) {
        child.readSuffix();
        return false;
    });
}

static constexpr size_t UNROLL_BATCH = 64;

template <int MODE>
Block DMVersionFilterBlockInputStream<MODE>::read(FilterPtr & res_filter, bool return_filter)
{
    while (true)
    {
        if (!raw_block)
        {
            if (!initNextBlock())
                return {};
        }

        Block cur_raw_block = raw_block;
        size_t rows = cur_raw_block.rows();

        if (cur_raw_block.getByPosition(handle_col_pos).column->isColumnConst())
        {
            // Clean read optimization.

            ++total_blocks;
            ++complete_passed;

            total_rows += rows;
            passed_rows += rows;

            initNextBlock();

            ProfileEvents::increment(ProfileEvents::DMCleanReadRows, rows);

            return getNewBlockByHeader(header, cur_raw_block);
        }

        filter.resize(rows);

        const size_t batch_rows = (rows - 1) / UNROLL_BATCH * UNROLL_BATCH;

        // The following is trying to unroll the filtering operations,
        // so that optimizer could use vectorized optimization.
        // The original logic can be seen in #checkWithNextIndex().

        if constexpr (MODE == DM_VERSION_FILTER_MODE_MVCC)
        {
            /// filter[i] = !deleted && cur_version <= version_limit && (cur_handle != next_handle || next_version > version_limit)
            {
                UInt8 * filter_pos = filter.data();
                auto * version_pos = const_cast<UInt64 *>(version_col_data->data()) + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) = (*version_pos) > version_limit;

                    ++filter_pos;
                    ++version_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                size_t handle_pos = 0;
                size_t next_handle_pos = handle_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos)
                        |= compare(rowkey_column->getRowKeyValue(handle_pos), rowkey_column->getRowKeyValue(next_handle_pos)) != 0;
                    ++filter_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                auto * version_pos = const_cast<UInt64 *>(version_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) &= (*version_pos) <= version_limit;

                    ++filter_pos;
                    ++version_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                auto * delete_pos = const_cast<UInt8 *>(delete_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) &= !(*delete_pos);

                    ++filter_pos;
                    ++delete_pos;
                }
            }
        }
        else if constexpr (MODE == DM_VERSION_FILTER_MODE_COMPACT)
        {
            /// filter[i] = cur_version >= version_limit || ((cur_handle != next_handle || next_version > version_limit) && !deleted);

            {
                UInt8 * filter_pos = filter.data();
                size_t handle_pos = 0;
                size_t next_handle_pos = handle_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) = compare(rowkey_column->getRowKeyValue(handle_pos), rowkey_column->getRowKeyValue(next_handle_pos)) != 0;
                    ++filter_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                auto * version_pos = const_cast<UInt64 *>(version_col_data->data());
                auto * next_version_pos = version_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) |= (*next_version_pos) > version_limit;

                    ++filter_pos;
                    ++next_version_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                auto * delete_pos = const_cast<UInt8 *>(delete_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) &= !(*delete_pos);

                    ++filter_pos;
                    ++delete_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                auto * version_pos = const_cast<UInt64 *>(version_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) |= (*version_pos) >= version_limit;

                    ++filter_pos;
                    ++version_pos;
                }
            }

            // Let's set effective.
            effective.resize(rows);

            {
                UInt8 * effective_pos = effective.data();
                size_t handle_pos = 0;
                size_t next_handle_pos = handle_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*effective_pos)
                        = compare(rowkey_column->getRowKeyValue(handle_pos), rowkey_column->getRowKeyValue(next_handle_pos)) != 0;
                    ++effective_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                }
            }

            {
                UInt8 * effective_pos = effective.data();
                UInt8 * filter_pos = filter.data();
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*effective_pos) &= (*filter_pos);

                    ++effective_pos;
                    ++filter_pos;
                }
            }

            // Let's set not_clean.
            not_clean.resize(rows);

            {
                UInt8 * not_clean_pos = not_clean.data();
                size_t handle_pos = 0;
                size_t next_handle_pos = handle_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*not_clean_pos)
                        = compare(rowkey_column->getRowKeyValue(handle_pos), rowkey_column->getRowKeyValue(next_handle_pos)) == 0;
                    ++not_clean_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                }
            }

            {
                UInt8 * not_clean_pos = not_clean.data();
                auto * delete_pos = const_cast<UInt8 *>(delete_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*not_clean_pos) |= (*delete_pos);

                    ++not_clean_pos;
                    ++delete_pos;
                }
            }

            {
                UInt8 * not_clean_pos = not_clean.data();
                UInt8 * filter_pos = filter.data();
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*not_clean_pos) &= (*filter_pos);

                    ++not_clean_pos;
                    ++filter_pos;
                }
            }

            // Let's calculate gc_hint_version
            gc_hint_version = UINT64_MAX;
            {
                UInt8 * filter_pos = filter.data();
                size_t handle_pos = 0;
                size_t next_handle_pos = handle_pos + 1;
                auto * version_pos = const_cast<UInt64 *>(version_col_data->data());
                auto * delete_pos = const_cast<UInt8 *>(delete_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    if (*filter_pos)
                        gc_hint_version = std::min(gc_hint_version,
                                                   calculateRowGcHintVersion(rowkey_column->getRowKeyValue(handle_pos),
                                                                             *version_pos,
                                                                             rowkey_column->getRowKeyValue(next_handle_pos),
                                                                             true,
                                                                             *delete_pos));

                    ++filter_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                    ++version_pos;
                    ++delete_pos;
                }
            }
        }
        else
        {
            throw Exception("Unsupported mode");
        }

        for (size_t i = batch_rows; i < rows - 1; ++i)
            checkWithNextIndex(i);

        {
            // Now let's handle the last row of current block.
            auto cur_handle = rowkey_column->getRowKeyValue(rows - 1);
            auto cur_version = (*version_col_data)[rows - 1];
            auto deleted = (*delete_col_data)[rows - 1];
            if (!initNextBlock())
            {
                // No more block.
                if constexpr (MODE == DM_VERSION_FILTER_MODE_MVCC)
                {
                    filter[rows - 1] = !deleted && cur_version <= version_limit;
                }
                else if (MODE == DM_VERSION_FILTER_MODE_COMPACT)
                {
                    filter[rows - 1] = cur_version >= version_limit || !deleted;
                    not_clean[rows - 1] = filter[rows - 1] && deleted;
                    effective[rows - 1] = filter[rows - 1];
                    if (filter[rows - 1])
                        gc_hint_version = std::min(
                            gc_hint_version,
                            calculateRowGcHintVersion(cur_handle, cur_version, /* just a placeholder */ cur_handle, false, deleted));
                }
                else
                {
                    throw Exception("Unsupported mode");
                }
            }
            else
            {
                auto next_handle = rowkey_column->getRowKeyValue(0);
                auto next_version = (*version_col_data)[0];
                if constexpr (MODE == DM_VERSION_FILTER_MODE_MVCC)
                {
                    filter[rows - 1] = !deleted && cur_version <= version_limit
                        && (compare(cur_handle, next_handle) != 0 || next_version > version_limit);
                }
                else if (MODE == DM_VERSION_FILTER_MODE_COMPACT)
                {
                    filter[rows - 1] = cur_version >= version_limit
                        || ((compare(cur_handle, next_handle) != 0 || next_version > version_limit) && !deleted);
                    not_clean[rows - 1] = filter[rows - 1] && (compare(cur_handle, next_handle) == 0 || deleted);
                    effective[rows - 1] = filter[rows - 1] && (compare(cur_handle, next_handle) != 0);
                    if (filter[rows - 1])
                        gc_hint_version
                            = std::min(gc_hint_version, calculateRowGcHintVersion(cur_handle, cur_version, next_handle, true, deleted));
                }
                else
                {
                    throw Exception("Unsupported mode");
                }
            }
        }

        const size_t passed_count = countBytesInFilter(filter);

        if constexpr (MODE == DM_VERSION_FILTER_MODE_COMPACT)
        {
            not_clean_rows += countBytesInFilter(not_clean);
            effective_num_rows += countBytesInFilter(effective);
        }

        ++total_blocks;
        total_rows += rows;
        passed_rows += passed_count;

        // This block is empty after filter, continue to process next block
        if (passed_count == 0)
        {
            ++complete_not_passed;
            continue;
        }

        if (passed_count == rows)
        {
            ++complete_passed;
            return getNewBlockByHeader(header, cur_raw_block);
        }

        if (return_filter)
        {
            // The caller of this method should do the filtering, we just need to return the original block.
            res_filter = &filter;
            return getNewBlockByHeader(header, cur_raw_block);
        }
        else
        {
            Block res;
            for (auto & c : header)
            {
                auto & column = cur_raw_block.getByName(c.name);
                column.column = column.column->filter(filter, passed_count);
                res.insert(std::move(column));
            }
            return res;
        }
    }
}

template class DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>;
template class DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>;

} // namespace DM
} // namespace DB
