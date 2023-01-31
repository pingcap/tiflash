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
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/HashPartitionWriterV1.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
constexpr ssize_t MAX_BATCH_SEND_MIN_LIMIT_MEM_SIZE = 1024 * 1024 * 64; // 64MB: 8192 Rows * 256 Byte/row * 32 partitions

template <class ExchangeWriterPtr>
HashPartitionWriterV1<ExchangeWriterPtr>::HashPartitionWriterV1(
    ExchangeWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    Int64 partition_batch_limit_,
    DAGContext & dag_context_,
    tipb::CompressionMode compression_mode_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , partition_num(writer_->getPartitionNum())
    , batch_send_min_limit(partition_batch_limit_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , compression_method(ToInternalCompressionMethod(compression_mode_))
{
    rows_in_blocks = 0;
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);

    {
        // make `batch_send_min_limit` always GT 0
        if (batch_send_min_limit <= 0)
        {
            // set upper limit if not specified
            batch_send_min_limit = 8 * 1024 * partition_num /* 8K * partition-num */;
        }
        for (const auto & field_type : dag_context.result_field_types)
        {
            expected_types.emplace_back(getDataTypeByFieldTypeForComputingLayer(field_type));
        }
    }
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        partitionAndEncodeThenWriteBlocks();
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    if (rows > 0)
    {
        rows_in_blocks += rows;
        mem_size_in_blocks += block.bytes();
        blocks.push_back(block);
    }
    if (static_cast<Int64>(rows_in_blocks) >= batch_send_min_limit
        || mem_size_in_blocks >= MAX_BATCH_SEND_MIN_LIMIT_MEM_SIZE)
        partitionAndEncodeThenWriteBlocks();
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::partitionAndEncodeThenWriteBlocks()
{
    assert(rows_in_blocks > 0);
    assert(mem_size_in_blocks > 0);
    assert(!blocks.empty());

    HashBaseWriterHelper::materializeBlocks(blocks);
    // All blocks are same, use one block's meta info as header
    Block dest_block_header = blocks.back().cloneEmpty();
    std::vector<String> partition_key_containers(collators.size());
    std::vector<std::vector<MutableColumns>> dest_columns(partition_num);
    size_t total_rows = 0;

    while (!blocks.empty())
    {
        const auto & block = blocks.back();
        {
            // check schema
            assertBlockSchema(expected_types, block, "HashPartitionWriterV1");
        }
        auto && dest_tbl_cols = HashBaseWriterHelper::createDestColumns(block, partition_num);
        HashBaseWriterHelper::scatterColumns(block, partition_col_ids, collators, partition_key_containers, partition_num, dest_tbl_cols);
        blocks.pop_back();

        for (size_t part_id = 0; part_id < partition_num; ++part_id)
        {
            auto & columns = dest_tbl_cols[part_id];
            if unlikely (!columns.front())
                continue;
            size_t expect_size = columns.front()->size();
            total_rows += expect_size;
            dest_columns[part_id].emplace_back(std::move(columns));
        }
    }
    RUNTIME_CHECK(rows_in_blocks, total_rows);

    for (size_t part_id = 0; part_id < partition_num; ++part_id)
    {
        auto is_local = writer->isLocal(part_id);
        auto method = is_local ? CompressionMethod::NONE : compression_method;
        CHBlockChunkCodecV1 codec{dest_block_header};
        auto && res = codec.encode(std::move(dest_columns[part_id]), method);
        if (res.empty())
            continue;

        auto tracked_packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV1);
        tracked_packet->getPacket().add_chunks(std::move(res));

        auto & inner_packet = tracked_packet->getPacket();
        auto sz = inner_packet.ByteSizeLong();
        {
            writer->partitionWrite(std::move(tracked_packet), part_id);
            updateHashPartitionWriterMetrics(compression_method, sz, writer->isLocal(part_id));
            GET_METRIC(tiflash_exchange_data_bytes, type_hash_original).Increment(codec.original_size);
        }
    }

    assert(blocks.empty());
    rows_in_blocks = 0;
    mem_size_in_blocks = 0;
}

void updateHashPartitionWriterMetrics(CompressionMethod method, size_t sz, bool is_local)
{
    if (is_local)
    {
        method = CompressionMethod::NONE;
    }

    switch (method)
    {
    case CompressionMethod::NONE:
    {
        if (is_local)
        {
            GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_local).Increment(sz);
        }
        else
        {
            GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_remote).Increment(sz);
        }
        break;
    }
    case CompressionMethod::LZ4:
    {
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_lz4_compression).Increment(sz);
        break;
    }
    case CompressionMethod::ZSTD:
    {
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_zstd_compression).Increment(sz);
        break;
    }
    default:
        break;
    }
}

template class HashPartitionWriterV1<MPPTunnelSetPtr>;

} // namespace DB
