#include <Common/Decimal.h>
#include <Common/FieldVisitors.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>

#include "common/logger_useful.h"

namespace ProfileEvents
{
extern const Event DMCleanReadRows;
} // namespace ProfileEvents

namespace DB
{

namespace DM
{

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

        Block  cur_raw_block = raw_block;
        size_t rows          = cur_raw_block.rows();


        auto log_if_exist_suspicious_row = [&](const Block & block_to_ret, IColumn::Filter * filter_to_ret) {
            constexpr std::string_view table_to_check     = "t_45";
            constexpr ColId            col_id_price       = 21;
            constexpr ColId            col_id_combo_price = 23;

            // constexpr std::string_view table_to_check = "t_705";
            // constexpr ColId col_id_price       = 21;
            // constexpr ColId col_id_combo_price = 50;
            if (table_name != table_to_check)
            {
                return;
            }

            // 0x400000000 == 1717986.9184 * 10000
            const DecimalField<Decimal64> suspicious_val(static_cast<Int64>(0x400000000), 4);
            const DataTypeDecimal64       suspicious_data_type(18, 4);
            const auto                    suspicious_const_col = suspicious_data_type.createColumn();
            suspicious_const_col->insert(suspicious_val);
            auto log_block_row = [](const Block &     block_to_dump,
                                    IColumn::Filter * filter_to_dump,
                                    size_t            row_index,
                                    UInt64            tso,
                                    const String &    table_name,
                                    Poco::Logger *    log) {
                Field  col_field;
                String row_to_dump;
                for (auto & col_with_n : block_to_dump)
                {
                    col_with_n.column->get(row_index, col_field);
                    // "${col_name}_${col_val},"
                    row_to_dump += col_with_n.name + "_" + applyVisitor(FieldVisitorDump(), col_field) + ",";
                }
                // this row has been filtered by the filter or not
                if (filter_to_dump == nullptr)
                {
                    row_to_dump += "filter_<null>";
                }
                else
                {
                    row_to_dump += "filter_" + DB::toString(filter_to_dump->data()[row_index]);
                }
                LOG_WARNING(log,
                            "Found suspicious row!!! [table_name=" << table_name << "] [tso=" << tso
                                                                   << "] [num_rows=" << block_to_dump.rows() << "] [row_index=" << row_index
                                                                   << "] [" << row_to_dump << "]");
            };

            // find if there exist any of target column id

            for (auto c_iter = block_to_ret.begin(); c_iter != block_to_ret.end(); ++c_iter)
            {
                const auto & c              = *c_iter;
                auto         unwrapped_type = removeNullable(c.type);
                if (unwrapped_type->isDecimal() && (c.column_id == col_id_price || c.column_id == col_id_combo_price))
                {
                    auto unwrapped_col = c.column;
                    if (c.column->isColumnNullable())
                    {
                        const auto * null_input_column = checkAndGetColumn<ColumnNullable>(c.column.get());
                        unwrapped_col                  = null_input_column->getNestedColumnPtr();
                    }

                    for (size_t row_idx = 0; row_idx < unwrapped_col->size(); ++row_idx)
                    {
                        // check whether the row value is suspicious
                        const int compare_res = unwrapped_col->compareAt(row_idx, 0, *suspicious_const_col, 0);
                        // greater or equal to suspicious value
                        if (compare_res >= 0)
                        {
                            log_block_row(block_to_ret, filter_to_ret, row_idx, version_limit, table_name, log);
                        }
                    }
                }
            }
        };

        if (cur_raw_block.getByPosition(handle_col_pos).column->isColumnConst())
        {
            // Clean read optimization.

            ++total_blocks;
            ++complete_passed;

            total_rows += rows;
            passed_rows += rows;

            initNextBlock();

            ProfileEvents::increment(ProfileEvents::DMCleanReadRows, rows);

            auto res = getNewBlockByHeader(header, cur_raw_block);

            log_if_exist_suspicious_row(res, nullptr);

            return res;
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
                UInt8 * filter_pos  = filter.data();
                auto *  version_pos = const_cast<UInt64 *>(version_col_data->data()) + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) = (*version_pos) > version_limit;

                    ++filter_pos;
                    ++version_pos;
                }
            }

            {
                UInt8 * filter_pos      = filter.data();
                auto *  handle_pos      = const_cast<Handle *>(handle_col_data->data());
                auto *  next_handle_pos = handle_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) |= (*handle_pos) != (*(next_handle_pos));

                    ++filter_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                }
            }

            {
                UInt8 * filter_pos  = filter.data();
                auto *  version_pos = const_cast<UInt64 *>(version_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) &= (*version_pos) <= version_limit;

                    ++filter_pos;
                    ++version_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                auto *  delete_pos = const_cast<UInt8 *>(delete_col_data->data());
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
                UInt8 * filter_pos      = filter.data();
                auto *  handle_pos      = const_cast<Handle *>(handle_col_data->data());
                auto *  next_handle_pos = handle_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) = (*handle_pos) != (*(next_handle_pos));

                    ++filter_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                }
            }

            {
                UInt8 * filter_pos       = filter.data();
                auto *  version_pos      = const_cast<UInt64 *>(version_col_data->data());
                auto *  next_version_pos = version_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) |= (*next_version_pos) > version_limit;

                    ++filter_pos;
                    ++next_version_pos;
                }
            }

            {
                UInt8 * filter_pos = filter.data();
                auto *  delete_pos = const_cast<UInt8 *>(delete_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) &= !(*delete_pos);

                    ++filter_pos;
                    ++delete_pos;
                }
            }

            {
                UInt8 * filter_pos  = filter.data();
                auto *  version_pos = const_cast<UInt64 *>(version_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) |= (*version_pos) >= version_limit;

                    ++filter_pos;
                    ++version_pos;
                }
            }

            // Let's set not_clean.
            not_clean.resize(rows);

            {
                UInt8 * not_clean_pos   = not_clean.data();
                auto *  handle_pos      = const_cast<Handle *>(handle_col_data->data());
                auto *  next_handle_pos = handle_pos + 1;
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*not_clean_pos) = (*handle_pos) == (*(next_handle_pos));

                    ++not_clean_pos;
                    ++handle_pos;
                    ++next_handle_pos;
                }
            }

            {
                UInt8 * not_clean_pos = not_clean.data();
                auto *  delete_pos    = const_cast<UInt8 *>(delete_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*not_clean_pos) |= (*delete_pos);

                    ++not_clean_pos;
                    ++delete_pos;
                }
            }

            {
                UInt8 * not_clean_pos = not_clean.data();
                UInt8 * filter_pos    = filter.data();
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*not_clean_pos) &= (*filter_pos);

                    ++not_clean_pos;
                    ++filter_pos;
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
            auto cur_handle  = (*handle_col_data)[rows - 1];
            auto cur_version = (*version_col_data)[rows - 1];
            auto deleted     = (*delete_col_data)[rows - 1];
            if (!initNextBlock())
            {
                // No more block.
                if constexpr (MODE == DM_VERSION_FILTER_MODE_MVCC)
                {
                    filter[rows - 1] = !deleted && cur_version <= version_limit;
                }
                else if (MODE == DM_VERSION_FILTER_MODE_COMPACT)
                {
                    filter[rows - 1]    = cur_version >= version_limit || !deleted;
                    not_clean[rows - 1] = filter[rows - 1] && deleted;
                }
                else
                {
                    throw Exception("Unsupported mode");
                }
            }
            else
            {
                auto next_handle  = (*handle_col_data)[0];
                auto next_version = (*version_col_data)[0];
                if constexpr (MODE == DM_VERSION_FILTER_MODE_MVCC)
                {
                    filter[rows - 1]
                        = !deleted && cur_version <= version_limit && (cur_handle != next_handle || next_version > version_limit);
                }
                else if (MODE == DM_VERSION_FILTER_MODE_COMPACT)
                {
                    filter[rows - 1]
                        = cur_version >= version_limit || ((cur_handle != next_handle || next_version > version_limit) && !deleted);
                    not_clean[rows - 1] = filter[rows - 1] && (cur_handle == next_handle || deleted);
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
            auto res = getNewBlockByHeader(header, cur_raw_block);

            log_if_exist_suspicious_row(res, nullptr);

            return res;
        }

        if (return_filter)
        {
            // The caller of this method should do the filtering, we just need to return the original block.
            res_filter = &filter;
            auto res   = getNewBlockByHeader(header, cur_raw_block);

            log_if_exist_suspicious_row(res, &filter);

            return res;
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

            log_if_exist_suspicious_row(res, &filter);

            return res;
        }
    }
}

template class DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>;
template class DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>;

} // namespace DM
} // namespace DB
