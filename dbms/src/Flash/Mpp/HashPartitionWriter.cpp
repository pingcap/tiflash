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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/HashPartitionWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{
template <class ExchangeWriterPtr>
HashPartitionWriter<ExchangeWriterPtr>::HashPartitionWriter(
    ExchangeWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    Int64 batch_send_min_limit_,
    DAGContext & dag_context_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        partitionAndWriteBlocks();
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    if (rows > 0)
    {
        rows_in_blocks += rows;
        blocks.push_back(block);
    }

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        partitionAndWriteBlocks();
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::partitionAndWriteBlocks()
{
    std::vector<Blocks> partition_blocks;
    partition_blocks.resize(partition_num);

    if (!blocks.empty())
    {
        assert(rows_in_blocks > 0);

        HashBaseWriterHelper::materializeBlocks(blocks);
        std::vector<String> partition_key_containers(collators.size());

        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            auto dest_tbl_cols = HashBaseWriterHelper::createDestColumns(block, partition_num);
            HashBaseWriterHelper::scatterColumns(block, partition_col_ids, collators, partition_key_containers, partition_num, dest_tbl_cols);
            blocks.pop_back();

            for (size_t part_id = 0; part_id < partition_num; ++part_id)
            {
                Block dest_block = blocks[0].cloneEmpty();
                dest_block.setColumns(std::move(dest_tbl_cols[part_id]));
                size_t dest_block_rows = dest_block.rows();
                if (dest_block_rows > 0)
                    partition_blocks[part_id].push_back(std::move(dest_block));
            }
        }
        assert(blocks.empty());
        rows_in_blocks = 0;
    }

    writePartitionBlocks(partition_blocks);
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::writePartitionBlocks(std::vector<Blocks> & partition_blocks)
{
    for (size_t part_id = 0; part_id < partition_num; ++part_id)
    {
        auto & blocks = partition_blocks[part_id];
        if (likely(blocks.size() > 0))
        {
            writer->partitionWrite(blocks, part_id);
            blocks.clear();
        }
    }
}

template class HashPartitionWriter<MPPTunnelSetPtr>;

} // namespace DB
