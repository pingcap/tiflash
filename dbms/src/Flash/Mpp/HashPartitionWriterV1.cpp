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
extern size_t ApproxBlockBytes(const Block & block);
} // namespace DB

namespace DB
{
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
    , partition_batch_limit(partition_batch_limit_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , compression_method(ToInternalCompressionMethod(compression_mode_))
{
    if (partition_batch_limit < 0)
    {
        partition_batch_limit = 8192LL * partition_num;
    }

    rows_in_blocks = 0;
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
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
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
    }

    if (static_cast<Int64>(rows_in_blocks) > partition_batch_limit)
        partitionAndEncodeThenWriteBlocks();
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::partitionAndEncodeThenWriteBlocks()
{
    if (blocks.empty())
        return;

    // Set mpp packet data version to `1`
    auto tracked_packets = HashBaseWriterHelper::createPackets(partition_num, DB::MPPDataPacketV1);

    size_t ori_all_packets_size = 0;
    {
        assert(rows_in_blocks > 0);

        HashBaseWriterHelper::materializeBlocks(blocks);

        // All blocks are same, use one block's meta info as header
        Block dest_block_header = blocks.back().cloneEmpty();

        std::vector<String> partition_key_containers(collators.size());
        std::vector<std::vector<MutableColumns>> dest_columns(partition_num);
        [[maybe_unused]] size_t total_rows = 0;

        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            block.checkNumberOfRows();

            total_rows += block.rows();

            auto && dest_tbl_cols = HashBaseWriterHelper::createDestColumns(block, partition_num);
            HashBaseWriterHelper::scatterColumns(block, partition_col_ids, collators, partition_key_containers, partition_num, dest_tbl_cols);
            blocks.pop_back();

            for (size_t part_id = 0; part_id < partition_num; ++part_id)
            {
                auto & columns = dest_tbl_cols[part_id];
                dest_columns[part_id].emplace_back(std::move(columns));
            }
        }

        auto && codec = CHBlockChunkCodecV1{
            dest_block_header,
            false /*scape empty part*/,
        };

        for (size_t part_id = 0; part_id < partition_num; ++part_id)
        {
            auto & part_columns = dest_columns[part_id];
            auto method = writer->isLocal(part_id) ? CompressionMethod::NONE : compression_method;
            auto && res = codec.encode(std::move(part_columns), method);
            if unlikely (res.empty())
                continue;

            tracked_packets[part_id]->getPacket().add_chunks(std::move(res));
        }
        assert(codec.encoded_rows == total_rows);
        assert(blocks.empty());

        ori_all_packets_size += codec.original_size;
        rows_in_blocks = 0;
    }

    writePackets(std::move(tracked_packets));

    GET_METRIC(tiflash_exchange_data_bytes, type_hash_original_all).Increment(ori_all_packets_size);
}

static void updateHashPartitionWriterMetrics(CompressionMethod method, size_t sz, bool is_local)
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
            GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_local).Increment(sz);
        }
        else
        {
            GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_remote).Increment(sz);
        }
        break;
    }
    case CompressionMethod::LZ4:
    {
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_lz4).Increment(sz);
        break;
    }
    case CompressionMethod::ZSTD:
    {
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_zstd).Increment(sz);
        break;
    }
    default:
        break;
    }
}

template <class ExchangeWriterPtr>
void WritePackets(CompressionMethod compression_method, TrackedMppDataPacketPtrs && packets, ExchangeWriterPtr & writer)
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        auto & packet = packets[part_id];
        assert(packet);

        auto & inner_packet = packet->getPacket();

        if (auto sz = inner_packet.ByteSizeLong(); likely(inner_packet.chunks_size() > 0))
        {
            writer->partitionWrite(std::move(packet), part_id);
            updateHashPartitionWriterMetrics(compression_method, sz, writer->isLocal(part_id));
        }
    }
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::writePackets(TrackedMppDataPacketPtrs && packets)
{
    WritePackets(compression_method, std::move(packets), writer);
}

template class HashPartitionWriterV1<MPPTunnelSetPtr>;

} // namespace DB
