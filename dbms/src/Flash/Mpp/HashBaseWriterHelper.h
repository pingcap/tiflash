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

#include <Core/Block.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <TiDB/Collation/Collator.h>

namespace DB::HashBaseWriterHelper
{
void materializeBlock(Block & input_block);
void materializeBlocks(std::vector<Block> & input_blocks);

std::vector<MutableColumns> createDestColumns(const Block & sample_block, size_t num);

/// Will resize 'hash' inside to ensure enough space
void computeHash(
    const Block & block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    WeakHash32 & hash);

/// Will resize 'hash' inside to ensure enough space
void computeHash(
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    WeakHash32 & hash);

void computeHashSelectiveBlock(
    const Block & block,
    const std::vector<Int64> & partition_id_cols,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    WeakHash32 & hash);

void scatterColumns(
    const Block & input_block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t bucket_num,
    std::vector<std::vector<MutableColumnPtr>> & result_columns);

void scatterColumnsSelectiveBlock(
    const Block & input_block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t bucket_num,
    std::vector<std::vector<MutableColumnPtr>> & result_columns);

void scatterColumnsForFineGrainedShuffle(
    const Block & block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t part_num,
    uint32_t fine_grained_shuffle_stream_count,
    WeakHash32 & hash,
    IColumn::Selector & selector,
    std::vector<IColumn::ScatterColumns> & scattered);

void scatterColumnsForFineGrainedShuffleSelectiveBlock(
    const Block & block,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    uint32_t part_num,
    uint32_t fine_grained_shuffle_stream_count,
    WeakHash32 & hash,
    IColumn::Selector & selector,
    std::vector<IColumn::ScatterColumns> & scattered);
} // namespace DB::HashBaseWriterHelper
