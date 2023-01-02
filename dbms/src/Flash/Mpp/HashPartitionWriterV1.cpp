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
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/HashPartitionWriterV1.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <kvproto/mpp.pb.h>

#include <cassert>
#include <cstddef>
#include <numeric>

#include "Common/Exception.h"
#include "Common/Stopwatch.h"
#include "Flash/Coprocessor/CHBlockChunkCodecStream.h"
#include "Flash/Coprocessor/CompressCHBlockChunkCodecStream.h"
#include "Flash/Coprocessor/CompressedCHBlockChunkCodec.h"
#include "Flash/Mpp/MppVersion.h"
#include "IO/CompressedStream.h"
#include "common/logger_useful.h"
#include "ext/scope_guard.h"

namespace DB
{
template <class ExchangeWriterPtr>
HashPartitionWriterV1<ExchangeWriterPtr>::HashPartitionWriterV1(
    ExchangeWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_,
    mpp::CompressMethod compress_method_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , compress_method(compress_method_)
{
    assert(compress_method != mpp::CompressMethod::NONE);
    assert(dag_context.getMPPTaskMeta().mpp_version() > 0);

    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    for (const auto & field_type : dag_context.result_field_types)
    {
        expected_types.emplace_back(getDataTypeByFieldTypeForComputingLayer(field_type));
    }
    compress_chunk_codec_stream = NewCompressCHBlockChunkCodecStream(ToInternalCompressionMethod(compress_method));
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::finishWrite()
{
    assert(0 == rows_in_blocks);
    if (should_send_exec_summary_at_last)
        sendExecutionSummary();
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::sendExecutionSummary()
{
    tipb::SelectResponse response;
    summary_collector.addExecuteSummaries(response);
    writer->sendExecutionSummary(response);
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

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        partitionAndEncodeThenWriteBlocks();
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::partitionAndEncodeThenWriteBlocks()
{
    assert(compress_chunk_codec_stream);

    auto tracked_packets = HashBaseWriterHelper::createPackets(partition_num, 1);

    for (size_t part_id = 0; part_id < partition_num; ++part_id)
    {
        auto method = compress_method;
        if (writer->isLocal(part_id))
        {
            method = mpp::CompressMethod::NONE;
            tracked_packets[part_id]->getPacket().set_version(0);
        }
        tracked_packets[part_id]->getPacket().mutable_compress()->set_method(method);
    }

    size_t ori_block_mem_size = 0;

    if (!blocks.empty())
    {
        assert(rows_in_blocks > 0);

        HashBaseWriterHelper::materializeBlocks(blocks);
        Block dest_block_header = blocks[0].cloneEmpty();
        assertBlockSchema(expected_types, dest_block_header, "HashPartitionWriterV1");

        std::vector<String> partition_key_containers(collators.size());
        std::vector<std::vector<MutableColumns>> dest_columns(partition_num);
        size_t total_rows = 0, encoded_rows = 0;

        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            block.checkNumberOfRows();
            assertBlockSchema(expected_types, block, "HashPartitionWriterV1");

            ori_block_mem_size += ApproxBlockBytes(block);
            total_rows += block.rows();

            auto dest_tbl_cols = HashBaseWriterHelper::createDestColumns(block, partition_num);
            HashBaseWriterHelper::scatterColumns(block, partition_num, collators, partition_key_containers, partition_col_ids, dest_tbl_cols);
            blocks.pop_back();

            for (size_t part_id = 0; part_id < partition_num; ++part_id)
            {
                auto & columns = dest_tbl_cols[part_id];
                dest_columns[part_id].emplace_back(std::move(columns));
            }
        }

        for (size_t part_id = 0; part_id < partition_num; ++part_id)
        {
            if (tracked_packets[part_id]->getPacket().compress().method() == mpp::NONE)
            {
                auto * ostr_ptr = compress_chunk_codec_stream->getWriterWithoutCompress();
                for (auto && columns : dest_columns[part_id])
                {
                    dest_block_header.setColumns(std::move(columns));
                    encoded_rows += dest_block_header.rows();

                    EncodeCHBlockChunk(ostr_ptr, dest_block_header);
                    tracked_packets[part_id]->getPacket().add_chunks(ostr_ptr->getString());
                    ostr_ptr->reset();

                    {
                        const auto & chunks = tracked_packets[part_id]->getPacket().chunks();
                        const auto & dd = chunks[chunks.size() - 1];

                        auto res = CHBlockChunkCodec::decode(dd, dest_block_header);
                        RUNTIME_CHECK(res.rows() == dest_block_header.rows(), res.rows(), dest_block_header.rows());
                        RUNTIME_CHECK(res.columns() == dest_block_header.columns(), res.columns(), dest_block_header.columns());
                        res.checkNumberOfRows();
                        for (size_t i = 0; i < res.columns(); ++i)
                        {
                            RUNTIME_CHECK(dest_block_header.getByPosition(i) == res.getByPosition(i));
                        }
                    }
                }
            }
            else
            {
                size_t part_rows = std::accumulate(dest_columns[part_id].begin(), dest_columns[part_id].end(), 0, [](const auto & r, const auto & columns) { return r + columns[0]->size(); });
                encoded_rows += part_rows;

                compress_chunk_codec_stream->encodeHeader(dest_block_header, part_rows);
                for (size_t col_index = 0; col_index < dest_block_header.columns(); ++col_index)
                {
                    auto && col_type_name = dest_block_header.getByPosition(col_index);
                    for (auto && columns : dest_columns[part_id])
                    {
                        compress_chunk_codec_stream->encodeColumn(std::move(columns[col_index]), col_type_name);
                    }
                }

                tracked_packets[part_id]->getPacket().add_chunks(compress_chunk_codec_stream->getString());
                compress_chunk_codec_stream->reset();

                {
                    // decode
                    const auto & chunks = tracked_packets[part_id]->getPacket().chunks();
                    const auto & dd = chunks[chunks.size() - 1];

                    ReadBufferFromString istr(dd);
                    auto && compress_buffer = CompressedCHBlockChunkCodec::CompressedReadBuffer(istr);

                    size_t rows{};
                    Block res = DecodeHeader(compress_buffer, dest_block_header, rows);
                    DecodeColumns(compress_buffer, res, res.columns(), rows);
                    RUNTIME_CHECK(res.rows() == part_rows, res.rows(), part_rows);
                    RUNTIME_CHECK(res.columns() == dest_block_header.columns(), res.columns(), dest_block_header.columns());
                    res.checkNumberOfRows();
                    for (size_t i = 0; i < res.columns(); ++i)
                    {
                        RUNTIME_CHECK(dest_block_header.getByPosition(i) == res.getByPosition(i));
                    }
                }
            }
        }

        RUNTIME_CHECK(encoded_rows == total_rows, encoded_rows, total_rows);

        assert(blocks.empty());
        rows_in_blocks = 0;
    }

    writePackets(tracked_packets);

    GET_METRIC(tiflash_exchange_data_bytes, type_hash_original_all).Increment(ori_block_mem_size);
}

template <class ExchangeWriterPtr>
void HashPartitionWriterV1<ExchangeWriterPtr>::writePackets(const TrackedMppDataPacketPtrs & packets)
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        const auto & packet = packets[part_id];
        assert(packet);

        auto & inner_packet = packet->getPacket();
        if (likely(inner_packet.chunks_size() > 0))
        {
            writer->partitionWrite(packet, part_id);

            auto sz = inner_packet.ByteSizeLong();
            switch (inner_packet.compress().method())
            {
            case mpp::NONE:
            {
                if (writer->isLocal(part_id))
                {
                    GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_local).Increment(sz);
                }
                else
                {
                    GET_METRIC(tiflash_exchange_data_bytes, type_hash_none).Increment(sz);
                }
                break;
            }
            case mpp::LZ4:
            {
                GET_METRIC(tiflash_exchange_data_bytes, type_hash_lz4).Increment(sz);
                break;
            }
            case mpp::ZSTD:
            {
                GET_METRIC(tiflash_exchange_data_bytes, type_hash_zstd).Increment(sz);
                break;
            }
            default:
                break;
            }
        }
    }
}

template class HashPartitionWriterV1<MPPTunnelSetPtr>;

} // namespace DB
