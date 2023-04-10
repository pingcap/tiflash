// Copyright 2023 PingCAP, Ltd.
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
#include <Interpreters/JoinUtils.h>

namespace DB
{
void ProbeProcessInfo::resetBlock(Block && block_, size_t partition_index_)
{
    block = std::move(block_);
    partition_index = partition_index_;
    start_row = 0;
    end_row = 0;
    all_rows_joined_finish = false;
    // If the probe block size is greater than max_block_size, we will set max_block_size to the probe block size to avoid some unnecessary split.
    max_block_size = std::max(max_block_size, block.rows());
}

void ProbeProcessInfo::updateStartRow()
{
    assert(start_row <= end_row);
    start_row = end_row;
}
namespace
{
UInt64 inline updateHashValue(size_t restore_round, UInt64 x)
{
    static std::vector<UInt64> hash_constants{0xff51afd7ed558ccdULL, 0xc4ceb9fe1a85ec53ULL, 0xde43a68e4d184aa3ULL, 0x86f1fda459fa47c7ULL, 0xd91419add64f471fULL, 0xc18eea9cbe12489eULL, 0x2cb94f36b9fe4c38ULL, 0xef0f50cc5f0c4cbaULL};
    static size_t hash_constants_size = hash_constants.size();
    assert(hash_constants_size > 0 && (hash_constants_size & (hash_constants_size - 1)) == 0);
    assert(restore_round != 0);
    x ^= x >> 33;
    x *= hash_constants[restore_round & (hash_constants_size - 1)];
    x ^= x >> 33;
    x *= hash_constants[(restore_round + 1) & (hash_constants_size - 1)];
    x ^= x >> 33;
    return x;
}
} // namespace
void computeDispatchHash(size_t rows,
                         const ColumnRawPtrs & key_columns,
                         const TiDB::TiDBCollators & collators,
                         std::vector<String> & partition_key_containers,
                         size_t join_restore_round,
                         WeakHash32 & hash)
{
    HashBaseWriterHelper::computeHash(rows, key_columns, collators, partition_key_containers, hash);
    if (join_restore_round != 0)
    {
        auto & data = hash.getData();
        for (size_t i = 0; i < rows; ++i)
            data[i] = updateHashValue(join_restore_round, data[i]);
    }
}
} // namespace DB
