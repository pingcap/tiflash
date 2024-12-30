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

#include <Columns/countBytesInFilter.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <common/logger_useful.h>

#include <unordered_set>

namespace DB
{
namespace DM
{
/// DMDeleteFilterBlockInputStream is used to filter the column and filter out the rows whose del_mark is true
class DMDeleteFilterBlockInputStream : public IBlockInputStream
{
    static constexpr size_t UNROLL_BATCH = 64;

public:
    DMDeleteFilterBlockInputStream(
        const BlockInputStreamPtr & input,
        const ColumnDefines & columns_to_read_,
        const String & tracing_id = "")
        : columns_to_read(columns_to_read_)
        , header(toEmptyBlock(columns_to_read))
        , log(Logger::get(tracing_id))
    {
        children.emplace_back(input);
        delete_col_pos = input->getHeader().getPositionByName(TAG_COLUMN_NAME);
    }
    ~DMDeleteFilterBlockInputStream() override
    {
        LOG_TRACE(
            log,
            "Total rows: {}, pass: {:.2f}%"
            ", complete pass: {:.2f}%, complete not pass: {:.2f}%",
            total_rows,
            passed_rows * 100.0 / total_rows,
            complete_passed * 100.0 / total_blocks,
            complete_not_passed * 100.0 / total_blocks);
    }

    String getName() const override { return "DMDeleteFilter"; }

    Block getHeader() const override { return header; }

    Block read() override
    {
        while (true)
        {
            Block block = children.back()->read();
            if (!block)
                return {};
            if (block.rows() == 0)
                continue;

            /// if the pack is do clean read for del column, the del column returned is a const column, with size 1.
            /// In this case, all the del_mark must be 0. Thus we don't need extra filter.
            if (block.getByPosition(delete_col_pos).column->isColumnConst())
            {
                ++total_blocks;
                ++complete_passed;
                total_rows += block.rows();
                passed_rows += block.rows();

                return getNewBlockByHeader(header, block);
            }

            delete_col_data = getColumnVectorDataPtr<UInt8>(block, delete_col_pos);

            size_t rows = block.rows();
            delete_filter.resize(rows);

            const size_t batch_rows = (rows - 1) / UNROLL_BATCH * UNROLL_BATCH;

            // The following is trying to unroll the filtering operations,
            // so that optimizer could use vectorized optimization.
            {
                UInt8 * filter_pos = delete_filter.data();
                auto * delete_pos = const_cast<UInt8 *>(delete_col_data->data());
                for (size_t i = 0; i < batch_rows; ++i)
                {
                    (*filter_pos) = !(*delete_pos);
                    ++filter_pos;
                    ++delete_pos;
                }
            }

            for (size_t i = batch_rows; i < rows; ++i)
            {
                delete_filter[i] = !(*delete_col_data)[i];
            }

            const size_t passed_count = countBytesInFilter(delete_filter);

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
                return getNewBlockByHeader(header, block);
            }

            Block res;
            for (auto & cd : columns_to_read)
            {
                auto & column = block.getByName(cd.name);
                column.column = column.column->filter(delete_filter, passed_count);
                res.insert(std::move(column));
            }
            return res;
        }
    }

private:
    ColumnDefines columns_to_read;
    Block header;

    size_t delete_col_pos;

    IColumn::Filter delete_filter{};

    PaddedPODArray<UInt8> const * delete_col_data = nullptr;

    size_t total_blocks = 0;
    size_t total_rows = 0;
    size_t passed_rows = 0;
    size_t complete_passed = 0;
    size_t complete_not_passed = 0;

    LoggerPtr log;
};

class DMColumnProjectionBlockInputStream : public IBlockInputStream
{
public:
    DMColumnProjectionBlockInputStream(const BlockInputStreamPtr & input, const ColumnDefines & columns_to_read_)
        : columns_to_read(columns_to_read_)
        , header(toEmptyBlock(columns_to_read))
    {
        children.emplace_back(input);
    }

    String getName() const override { return "DMColumnProjection"; }

    Block getHeader() const override { return header; }

    Block read() override
    {
        Block block = children.back()->read();
        if (!block)
            return {};
        Block res;
        for (auto & cd : columns_to_read)
        {
            res.insert(block.getByName(cd.name));
        }
        return res;
    }

private:
    ColumnDefines columns_to_read;
    Block header;
};

class DMHandleConvertBlockInputStream : public IBlockInputStream
{
public:
    using ColumnNames = std::vector<std::string>;

    DMHandleConvertBlockInputStream(
        const BlockInputStreamPtr & input,
        const String & handle_name_,
        const DataTypePtr & handle_original_type_,
        const Context & context_)
        : handle_name(handle_name_)
        , handle_original_type(handle_original_type_)
        , context(context_)
    {
        children.emplace_back(input);
    }

    String getName() const override { return "DMHandleConvert"; }

    Block getHeader() const override { return children.back()->getHeader(); }

    Block read() override
    {
        Block block = children.back()->read();
        if (!block)
            return {};
        if (handle_original_type && block.has(handle_name))
        {
            auto pos = block.getPositionByName(handle_name);
            convertColumn(block, pos, handle_original_type, context);
            block.getByPosition(pos).type = handle_original_type;
        }
        return block;
    }

private:
    Block header;
    String handle_name;
    DataTypePtr handle_original_type;
    const Context & context;
};

} // namespace DM
} // namespace DB
