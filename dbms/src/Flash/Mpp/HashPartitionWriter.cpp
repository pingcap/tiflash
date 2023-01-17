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
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/HashPartitionWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
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
void HashPartitionWriter<ExchangeWriterPtr>::partitionAndEncodeThenWriteBlocksImplV1()
{
    assert(rows_in_blocks > 0);
    assert(codec_helper_v1);

    auto tracked_packets = HashBaseWriterHelper::createPackets(partition_num, data_codec_version);
    HashBaseWriterHelper::materializeBlocks(blocks);
    // All blocks are same, use one block's meta info as header
    Block dest_block_header = blocks.back().cloneEmpty();
    std::vector<String> partition_key_containers(collators.size());
    std::vector<std::vector<MutableColumns>> dest_columns(partition_num);

    while (!blocks.empty())
    {
        const auto & block = blocks.back();
        codec_helper_v1->checkBlock(block);
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
    assert(codec.encoded_rows == rows_in_blocks);
    assert(blocks.empty());

    rows_in_blocks = 0;

    WritePackets(compression_method, std::move(tracked_packets), writer);
    GET_METRIC(tiflash_exchange_data_bytes, type_hash_original_all).Increment(codec.original_size);
}

template <class ExchangeWriterPtr>
HashPartitionWriter<ExchangeWriterPtr>::~HashPartitionWriter() = default;

template <class ExchangeWriterPtr>
HashPartitionWriter<ExchangeWriterPtr>::HashPartitionWriter(
    ExchangeWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    Int64 batch_send_min_limit_,
    DAGContext & dag_context_,
    MPPDataPacketVersion data_codec_version_,
    tipb::CompressionMode compression_mode_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , data_codec_version(data_codec_version_)
    , compression_method(ToInternalCompressionMethod(compression_mode_))
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);

    switch (data_codec_version)
    {
    case MPPDataPacketV0:
    {
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    }
    default:
    {
        if (batch_send_min_limit < 0)
        {
            batch_send_min_limit = 8192LL * partition_num;
        }
        codec_helper_v1 = std::make_unique<HashBaseWriterHelper::HashPartitionWriterHelperV1>(dag_context.result_field_types);
        break;
    }
    }
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        partitionAndEncodeThenWriteBlocks();
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
        partitionAndEncodeThenWriteBlocks();
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::partitionAndEncodeThenWriteBlocks()
{
    if unlikely (blocks.empty())
        return;
    switch (data_codec_version)
    {
    case MPPDataPacketV0:
    {
        partitionAndEncodeThenWriteBlocksImpl();
        break;
    }
    default:
    {
        partitionAndEncodeThenWriteBlocksImplV1();
        break;
    }
    }
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::partitionAndEncodeThenWriteBlocksImpl()
{
    auto tracked_packets = HashBaseWriterHelper::createPackets(partition_num, data_codec_version);
    {
        assert(rows_in_blocks > 0);

        HashBaseWriterHelper::materializeBlocks(blocks);
        Block dest_block = blocks[0].cloneEmpty();
        std::vector<String> partition_key_containers(collators.size());

        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            auto dest_tbl_cols = HashBaseWriterHelper::createDestColumns(block, partition_num);
            HashBaseWriterHelper::scatterColumns(block, partition_col_ids, collators, partition_key_containers, partition_num, dest_tbl_cols);
            blocks.pop_back();

            for (size_t part_id = 0; part_id < partition_num; ++part_id)
            {
                dest_block.setColumns(std::move(dest_tbl_cols[part_id]));
                size_t dest_block_rows = dest_block.rows();
                if (dest_block_rows > 0)
                {
                    chunk_codec_stream->encode(dest_block, 0, dest_block_rows);
                    tracked_packets[part_id]->addChunk(chunk_codec_stream->getString());
                    chunk_codec_stream->clear();
                }
            }
        }
        assert(blocks.empty());
        rows_in_blocks = 0;
    }

    writePackets(tracked_packets);
}

template <class ExchangeWriterPtr>
void WritePackets(TrackedMppDataPacketPtrs & packets, ExchangeWriterPtr & writer)
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        auto & packet = packets[part_id];
        assert(packet);

        auto & inner_packet = packet->getPacket();

        if (const auto sz = inner_packet.ByteSizeLong(); likely(inner_packet.chunks_size() > 0))
        {
            writer->partitionWrite(std::move(packet), part_id);

            GET_METRIC(tiflash_exchange_data_bytes, type_hash_original_all).Increment(sz);
            if (writer->isLocal(part_id))
                GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_local).Increment(sz);
            else
                GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_remote).Increment(sz);
        }
    }
}

template <class ExchangeWriterPtr>
void HashPartitionWriter<ExchangeWriterPtr>::writePackets(TrackedMppDataPacketPtrs & packets)
{
    WritePackets(packets, writer);
}

template class HashPartitionWriter<MPPTunnelSetPtr>;

} // namespace DB
