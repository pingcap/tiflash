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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/FineGrainedShuffleWriter.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Mpp/MPPTunnelSetWriter.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{

const char * FineGrainedShuffleWriterLabels[] = {"FineGrainedShuffleWriter", "FineGrainedShuffleWriter-V1"};

template <class ExchangeWriterPtr>
FineGrainedShuffleWriter<ExchangeWriterPtr>::FineGrainedShuffleWriter(
    ExchangeWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    DAGContext & dag_context_,
    uint64_t fine_grained_shuffle_stream_count_,
    UInt64 fine_grained_shuffle_batch_size_,
    MPPDataPacketVersion data_codec_version_,
    tipb::CompressionMode compression_mode_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    , fine_grained_shuffle_batch_size(fine_grained_shuffle_batch_size_)
    , batch_send_row_limit(fine_grained_shuffle_batch_size * fine_grained_shuffle_stream_count)
    , hash(0)
    , data_codec_version(data_codec_version_)
    , compression_method(ToInternalCompressionMethod(compression_mode_))
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    RUNTIME_CHECK(partition_num > 0);
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::prepare(const Block & sample_block)
{
    /// Initialize header block, use column type to create new empty column to handle potential null column cases
    const auto & column_with_type_and_names = sample_block.getColumnsWithTypeAndName();
    for (const auto & column : column_with_type_and_names)
    {
        MutableColumnPtr empty_column = column.type->createColumn();
        ColumnWithTypeAndName new_column(std::move(empty_column), column.type, column.name);
        header.insert(new_column);
    }
    num_columns = header.columns();
    // fine_grained_shuffle_stream_count is in (0, 1024], and partition_num is uint16_t, so will not overflow.
    num_bucket = partition_num * fine_grained_shuffle_stream_count;
    partition_key_containers_for_reuse.resize(collators.size());
    initScatterColumns();

    switch (data_codec_version)
    {
    case MPPDataPacketV0:
        break;
    case MPPDataPacketV1:
    default:
    {
        for (const auto & field_type : dag_context.result_field_types)
        {
            expected_types.emplace_back(getDataTypeByFieldTypeForComputingLayer(field_type));
        }
        assertBlockSchema(expected_types, header, FineGrainedShuffleWriterLabels[MPPDataPacketV1]);
        break;
    }
    }

    prepared = true;
}

template <class ExchangeWriterPtr>
WriteResult FineGrainedShuffleWriter<ExchangeWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
    {
        auto wait_res = waitForWritable();
        if (wait_res == WaitResult::Ready)
        {
            batchWriteFineGrainedShuffle();
            return WriteResult::Done;
        }
        return wait_res == WaitResult::WaitForPolling ? WriteResult::NeedWaitForPolling
                                                      : WriteResult::NeedWaitForNotify;
    }
    return WriteResult::Done;
}

template <class ExchangeWriterPtr>
WaitResult FineGrainedShuffleWriter<ExchangeWriterPtr>::waitForWritable() const
{
    return writer->waitForWritable();
}

template <class ExchangeWriterPtr>
WriteResult FineGrainedShuffleWriter<ExchangeWriterPtr>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(prepared, "FineGrainedShuffleWriter should be prepared before writing.");
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");

    size_t rows = 0;
    if (block.info.selective)
        rows = block.info.selective->size();
    else
        rows = block.rows();

    if (rows > 0)
    {
        rows_in_blocks += rows;
        blocks.push_back(block);
    }

    if (blocks.size() == fine_grained_shuffle_stream_count
        || static_cast<UInt64>(rows_in_blocks) >= batch_send_row_limit)
    {
        return flush();
    }
    return WriteResult::Done;
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::initScatterColumns()
{
    scattered.resize(num_columns);
    for (size_t col_id = 0; col_id < num_columns; ++col_id)
    {
        auto & column = header.getByPosition(col_id).column;

        scattered[col_id].reserve(num_bucket);
        for (size_t chunk_id = 0; chunk_id < num_bucket; ++chunk_id)
        {
            scattered[col_id].emplace_back(column->cloneEmpty());
            scattered[col_id][chunk_id]->reserve(1024);
        }
    }
}

template <class ExchangeWriterPtr>
template <MPPDataPacketVersion version>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::batchWriteFineGrainedShuffleImpl()
{
    assert(!blocks.empty());

    {
        assert(rows_in_blocks > 0);
        assert(fine_grained_shuffle_stream_count <= 1024);

        HashBaseWriterHelper::materializeBlocks(blocks);
        for (auto & block : blocks)
        {
            if constexpr (version != MPPDataPacketV0)
            {
                // check schema
                assertBlockSchema(expected_types, block, FineGrainedShuffleWriterLabels[MPPDataPacketV1]);
            }

            if (block.info.selective)
                HashBaseWriterHelper::scatterColumnsForFineGrainedShuffleSelectiveBlock(
                    block,
                    partition_col_ids,
                    collators,
                    partition_key_containers_for_reuse,
                    partition_num,
                    fine_grained_shuffle_stream_count,
                    hash,
                    selector,
                    scattered);
            else
                HashBaseWriterHelper::scatterColumnsForFineGrainedShuffle(
                    block,
                    partition_col_ids,
                    collators,
                    partition_key_containers_for_reuse,
                    partition_num,
                    fine_grained_shuffle_stream_count,
                    hash,
                    selector,
                    scattered);
            block.clear();
        }
        blocks.clear();

        // serialize each partitioned block and write it to its destination
        size_t part_id = 0;
        for (size_t bucket_idx = 0; bucket_idx < num_bucket; bucket_idx += fine_grained_shuffle_stream_count, ++part_id)
        {
            writer->fineGrainedShuffleWrite(
                header,
                scattered,
                bucket_idx,
                fine_grained_shuffle_stream_count,
                num_columns,
                part_id,
                data_codec_version,
                compression_method);
        }
        rows_in_blocks = 0;
    }
}

template <class ExchangeWriterPtr>
void FineGrainedShuffleWriter<ExchangeWriterPtr>::batchWriteFineGrainedShuffle()
{
    switch (data_codec_version)
    {
    case MPPDataPacketV0:
    {
        batchWriteFineGrainedShuffleImpl<MPPDataPacketV0>();
        break;
    }
    case MPPDataPacketV1:
    default:
    {
        batchWriteFineGrainedShuffleImpl<MPPDataPacketV1>();
        break;
    }
    }
}

template class FineGrainedShuffleWriter<SyncMPPTunnelSetWriterPtr>;
template class FineGrainedShuffleWriter<AsyncMPPTunnelSetWriterPtr>;

} // namespace DB
