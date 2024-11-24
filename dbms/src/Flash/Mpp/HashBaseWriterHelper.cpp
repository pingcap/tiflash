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

#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <TiDB/Decode/TypeMapping.h>

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
        materializeBlock(block);
}

std::vector<MutableColumns> createDestColumns(const Block & sample_block, size_t num)
{
    std::vector<MutableColumns> dest_tbl_cols(num);
    for (auto & cols : dest_tbl_cols)
        cols = sample_block.cloneEmptyColumns();
    return dest_tbl_cols;
}

void fillSelector(size_t rows, const WeakHash32 & hash, uint32_t part_num, IColumn::Selector & selector)
{
    // fill selector array with most significant bits of hash values
    const auto & hash_data = hash.getData();
    RUNTIME_CHECK(rows == hash_data.size());

    selector.resize(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        /// Row from interval [(2^32 / part_num) * i, (2^32 / part_num) * (i + 1)) goes to partition with number i.
        selector[i] = hash_data[i]; /// [0, 2^32)
        selector[i] *= part_num; /// [0, part_num * 2^32), selector stores 64 bit values.
        selector[i] >>= 32u; /// [0, part_num)
    }
}

/// For FineGrainedShuffle, the selector algorithm should satisfy the requirement:
//  the FineGrainedShuffleStreamIndex can be calculated using hash_data and fine_grained_shuffle_stream_count values, without the presence of part_num.
void fillSelectorForFineGrainedShuffle(
    size_t rows,
    const WeakHash32 & hash,
    uint32_t part_num,
    uint32_t fine_grained_shuffle_stream_count,
    IColumn::Selector & selector)
{
    // fill selector array with most significant bits of hash values
    const auto & hash_data = hash.getData();
    selector.resize(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        /// Row from interval [(2^32 / part_num) * i, (2^32 / part_num) * (i + 1)) goes to partition with number i.
        selector[i] = hash_data[i]; /// [0, 2^32)
        selector[i] *= part_num; /// [0, part_num * 2^32), selector stores 64 bit values.
        selector[i] >>= 32u; /// [0, part_num)
        selector[i] = selector[i] * fine_grained_shuffle_stream_count
            + hash_data[i]
                % fine_grained_shuffle_stream_count; /// map to [0, part_num * fine_grained_shuffle_stream_count)
    }
}

void computeHash(
    const Block & block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    WeakHash32 & hash)
{
    size_t rows = block.rows();
    if unlikely (rows == 0)
        return;

    hash.reset(rows);
    /// compute hash values
    for (size_t i = 0; i < partition_col_ids.size(); ++i)
    {
        const auto & column = block.getByPosition(partition_col_ids[i]).column;
        column->updateWeakHash32(hash, collators[i], partition_key_containers[i]);
    }
}

void computeHashSelectiveBlock(
    const Block & block,
    const std::vector<Int64> & partition_id_cols,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    WeakHash32 & hash)
{
    RUNTIME_CHECK(block.info.selective && !block.info.selective->empty());
    const auto selective_rows = block.info.selective->size();

    hash.reset(selective_rows);
    for (size_t i = 0; i < partition_id_cols.size(); ++i)
    {
        const auto & column = block.getByPosition(partition_id_cols[i]).column;
        column->updateWeakHash32(hash, collators[i], partition_key_containers[i], *block.info.selective);
    }
}

void computeHash(
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    WeakHash32 & hash)
{
    if unlikely (rows == 0)
        return;

    hash.getData().resize(rows);
    hash.reset(rows);
    for (size_t i = 0; i < key_columns.size(); i++)
        key_columns[i]->updateWeakHash32(hash, collators[i], partition_key_containers[i]);
}

void scatterColumns(
    const Block & input_block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t bucket_num,
    std::vector<std::vector<MutableColumnPtr>> & result_columns)
{
    if unlikely (input_block.rows() == 0)
        return;

    WeakHash32 hash(0);
    computeHash(input_block, partition_col_ids, collators, partition_key_containers, hash);

    IColumn::Selector selector;
    fillSelector(input_block.rows(), hash, bucket_num, selector);

    for (size_t col_id = 0; col_id < input_block.columns(); ++col_id)
    {
        // Scatter columns to different partitions
        std::vector<MutableColumnPtr> part_columns
            = input_block.getByPosition(col_id).column->scatter(bucket_num, selector);
        assert(part_columns.size() == bucket_num);
        for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
        {
            result_columns[bucket_idx][col_id] = std::move(part_columns[bucket_idx]);
        }
    }
}

void scatterColumnsSelectiveBlock(
    const Block & input_block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t bucket_num,
    std::vector<std::vector<MutableColumnPtr>> & result_columns)
{
    RUNTIME_CHECK(input_block.info.selective && !input_block.info.selective->empty());

    WeakHash32 hash(0);
    computeHashSelectiveBlock(input_block, partition_col_ids, collators, partition_key_containers, hash);

    IColumn::Selector selector;
    fillSelector(input_block.info.selective->size(), hash, bucket_num, selector);

    for (size_t col_id = 0; col_id < input_block.columns(); ++col_id)
    {
        std::vector<MutableColumnPtr> part_columns
            = input_block.getByPosition(col_id).column->scatter(bucket_num, selector, *input_block.info.selective);
        assert(part_columns.size() == bucket_num);
        for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
        {
            result_columns[bucket_idx][col_id] = std::move(part_columns[bucket_idx]);
        }
    }
}

void scatterColumnsForFineGrainedShuffle(
    const Block & block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t part_num,
    uint32_t fine_grained_shuffle_stream_count,
    WeakHash32 & hash,
    IColumn::Selector & selector,
    std::vector<IColumn::ScatterColumns> & scattered)
{
    if unlikely (block.rows() == 0)
        return;

    // compute hash values
    computeHash(block, partition_col_ids, collators, partition_key_containers, hash);

    /// fill selector using computed hash
    fillSelectorForFineGrainedShuffle(block.rows(), hash, part_num, fine_grained_shuffle_stream_count, selector);

    // partition
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = block.getByPosition(i).column;
        column->scatterTo(scattered[i], selector);
    }
}

void scatterColumnsForFineGrainedShuffleSelectiveBlock(
    const Block & block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t part_num,
    uint32_t fine_grained_shuffle_stream_count,
    WeakHash32 & hash,
    IColumn::Selector & selector,
    std::vector<IColumn::ScatterColumns> & scattered)
{
    RUNTIME_CHECK(block.info.selective && !block.info.selective->empty());

    computeHashSelectiveBlock(block, partition_col_ids, collators, partition_key_containers, hash);

    fillSelectorForFineGrainedShuffle(
        block.info.selective->size(),
        hash,
        part_num,
        fine_grained_shuffle_stream_count,
        selector);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = block.getByPosition(i).column;
        column->scatterTo(scattered[i], selector, *block.info.selective);
    }
}

} // namespace DB::HashBaseWriterHelper
