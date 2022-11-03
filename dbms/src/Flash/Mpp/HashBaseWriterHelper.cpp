// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Mpp/HashBaseWriterHelper.h>

namespace DB::HashBaseWriterHelper
{
void materializeBlock(Block & input_block)
{
    for (size_t i = 0; i < input_block.columns(); ++i)
    {
        auto & element = input_block.getByPosition(i);
        auto & src = element.column;
        if (ColumnPtr converted = src->convertToFullColumnIfConst())
            src = converted;
    }
}

void materializeBlocks(std::vector<Block> & blocks)
{
    for (auto & block : blocks)
    {
        for (size_t i = 0; i < block.columns(); ++i)
        {
            auto & element = block.getByPosition(i);
            auto & src = element.column;
            if (ColumnPtr converted = src->convertToFullColumnIfConst())
                src = converted;
        }
    }
}

std::vector<MutableColumns> createDestColumns(const Block & sample_block, size_t num)
{
    std::vector<MutableColumns> dest_tbl_cols(num);
    for (auto & cols : dest_tbl_cols)
        cols = sample_block.cloneEmptyColumns();
    return dest_tbl_cols;
}

void computeHash(const Block & block,
                 uint32_t num_bucket,
                 const TiDB::TiDBCollators & collators,
                 std::vector<String> & partition_key_containers,
                 const std::vector<Int64> & partition_col_ids,
                 WeakHash32 & hash,
                 IColumn::Selector & selector)
{
    size_t num_rows = block.rows();
    // compute hash values
    for (size_t i = 0; i < partition_col_ids.size(); ++i)
    {
        const auto & column = block.getByPosition(partition_col_ids[i]).column;
        column->updateWeakHash32(hash, collators[i], partition_key_containers[i]);
    }

    // fill selector array with most significant bits of hash values
    const auto & hash_data = hash.getData();
    for (size_t i = 0; i < num_rows; ++i)
    {
        /// Row from interval [(2^32 / num_bucket) * i, (2^32 / num_bucket) * (i + 1)) goes to bucket with number i.
        selector[i] = hash_data[i]; /// [0, 2^32)
        selector[i] *= num_bucket; /// [0, num_bucket * 2^32), selector stores 64 bit values.
        selector[i] >>= 32u; /// [0, num_bucket)
    }
}

void scatterColumns(const Block & input_block,
                    uint32_t bucket_num,
                    const TiDB::TiDBCollators & collators,
                    std::vector<String> & partition_key_containers,
                    const std::vector<Int64> & partition_col_ids,
                    std::vector<std::vector<MutableColumnPtr>> & result_columns)
{
    size_t rows = input_block.rows();
    WeakHash32 hash(rows);
    IColumn::Selector selector(rows);
    computeHash(input_block, bucket_num, collators, partition_key_containers, partition_col_ids, hash, selector);

    for (size_t col_id = 0; col_id < input_block.columns(); ++col_id)
    {
        // Scatter columns to different partitions
        std::vector<MutableColumnPtr> part_columns = input_block.getByPosition(col_id).column->scatter(bucket_num, selector);
        assert(part_columns.size() == bucket_num);
        for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
        {
            result_columns[bucket_idx][col_id] = std::move(part_columns[bucket_idx]);
        }
    }
}

void scatterColumnsInplace(const Block & block,
                           uint32_t bucket_num,
                           const TiDB::TiDBCollators & collators,
                           std::vector<String> & partition_key_containers,
                           const std::vector<Int64> & partition_col_ids,
                           WeakHash32 & hash,
                           IColumn::Selector & selector,
                           std::vector<IColumn::ScatterColumns> & scattered)
{
    size_t num_rows = block.rows();
    // compute hash values
    hash.getData().resize(num_rows);
    hash.reset(num_rows);
    selector.resize(num_rows);
    computeHash(block, bucket_num, collators, partition_key_containers, partition_col_ids, hash, selector);

    // partition
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = block.getByPosition(i).column;
        column->scatterTo(scattered[i], selector);
    }
}

} // namespace DB::HashBaseWriterHelper