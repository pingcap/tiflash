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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTunnelSetHelper.h>
#include <Storages/Transaction/TiDB.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

#include <Flash/Mpp/BroadcastOrPassThroughWriter.cpp>
#include <Flash/Mpp/FineGrainedShuffleWriter.cpp>
#include <Flash/Mpp/HashPartitionWriter.cpp>

namespace DB
{
namespace tests
{
static CompressionMethodByte GetCompressionMethodByte(CompressionMethod m);

class TestMPPExchangeWriter : public testing::Test
{
protected:
    void SetUp() override
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        dag_context_ptr->encode_type = tipb::EncodeType::TypeCHBlock;
        dag_context_ptr->is_mpp_task = true;
        dag_context_ptr->is_root_mpp_task = false;
        dag_context_ptr->result_field_types = makeFields();
    }

public:
    TestMPPExchangeWriter()
        : part_col_ids{0}
        , part_col_collators{
              TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY)}
    {}

    // Return 10 Int64 column.
    static std::vector<tipb::FieldType> makeFields()
    {
        std::vector<tipb::FieldType> fields(10);
        for (int i = 0; i < 10; ++i)
        {
            fields[i].set_tp(TiDB::TypeLongLong);
            fields[i].set_flag(TiDB::ColumnFlagNotNull);
        }
        return fields;
    }

    // Return a block with **rows** and 10 Int64 column.
    static Block prepareUniformBlock(size_t rows)
    {
        std::vector<Int64> uniform_data_set;
        for (size_t i = 0; i < rows; ++i)
        {
            uniform_data_set.push_back(i);
        }
        Block block;
        for (int i = 0; i < 10; ++i)
        {
            DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
            MutableColumnPtr int64_col = int64_data_type->createColumn();
            for (Int64 r : uniform_data_set)
            {
                int64_col->insert(Field(r));
            }
            block.insert(ColumnWithTypeAndName{
                std::move(int64_col),
                int64_data_type,
                String("col") + std::to_string(i)});
        }
        return block;
    }

    // Return a block with **rows** and 10 Int64 column.
    static Block prepareRandomBlock(size_t rows)
    {
        Block block;
        for (size_t i = 0; i < 10; ++i)
        {
            DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
            auto int64_column = ColumnGenerator::instance().generate({rows, "Int64", RANDOM}).column;
            block.insert(ColumnWithTypeAndName{
                std::move(int64_column),
                int64_data_type,
                String("col") + std::to_string(i)});
        }
        return block;
    }


    std::vector<Int64> part_col_ids;
    TiDB::TiDBCollators part_col_collators;

    std::unique_ptr<DAGContext> dag_context_ptr;
};

using MockExchangeWriterChecker = std::function<void(const TrackedMppDataPacketPtr &, uint16_t)>;

struct MockExchangeWriter
{
    MockExchangeWriter(
        MockExchangeWriterChecker checker_,
        uint16_t part_num_,
        DAGContext & dag_context)
        : checker(checker_)
        , part_num(part_num_)
        , result_field_types(dag_context.result_field_types)
    {}
    void partitionWrite(
        const Block & header,
        std::vector<MutableColumns> && part_columns,
        int16_t part_id,
        MPPDataPacketVersion version,
        CompressionMethod method)
    {
        assert(version > MPPDataPacketV0);
        method = isLocal(part_id) ? CompressionMethod::NONE : method;
        size_t original_size = 0;
        auto tracked_packet = MPPTunnelSetHelper::ToPacket(header, std::move(part_columns), version, method, original_size);
        checker(tracked_packet, part_id);
    }
    void fineGrainedShuffleWrite(
        const Block & header,
        std::vector<IColumn::ScatterColumns> & scattered,
        size_t bucket_idx,
        UInt64 fine_grained_shuffle_stream_count,
        size_t num_columns,
        int16_t part_id,
        MPPDataPacketVersion version,
        CompressionMethod method)
    {
        if (version == MPPDataPacketV0)
            return fineGrainedShuffleWrite(header, scattered, bucket_idx, fine_grained_shuffle_stream_count, num_columns, part_id);
        method = isLocal(part_id) ? CompressionMethod::NONE : method;
        size_t original_size = 0;
        auto tracked_packet = MPPTunnelSetHelper::ToFineGrainedPacket(
            header,
            scattered,
            bucket_idx,
            fine_grained_shuffle_stream_count,
            num_columns,
            version,
            method,
            original_size);
        checker(tracked_packet, part_id);
    }

    void broadcastOrPassThroughWriteV0(Blocks & blocks)
    {
        checker(MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types), 0);
    }

    void broadcastWrite(Blocks & blocks)
    {
        return broadcastOrPassThroughWriteV0(blocks);
    }
    void passThroughWrite(Blocks & blocks)
    {
        return broadcastOrPassThroughWriteV0(blocks);
    }
    void broadcastOrPassThroughWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method)
    {
        if (version == MPPDataPacketV0)
            return broadcastOrPassThroughWriteV0(blocks);

        size_t original_size{};
        auto && packet = MPPTunnelSetHelper::ToPacket(std::move(blocks), version, compression_method, original_size);
        if (!packet)
            return;

        checker(packet, 0);
    }
    void broadcastWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method)
    {
        return broadcastOrPassThroughWrite(blocks, version, compression_method);
    }
    void passThroughWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method)
    {
        return broadcastOrPassThroughWrite(blocks, version, compression_method);
    }

    void partitionWrite(Blocks & blocks, uint16_t part_id)
    {
        checker(MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types), part_id);
    }
    void fineGrainedShuffleWrite(
        const Block & header,
        std::vector<IColumn::ScatterColumns> & scattered,
        size_t bucket_idx,
        uint16_t fine_grained_shuffle_stream_count,
        size_t num_columns,
        int16_t part_id)
    {
        auto tracked_packet = MPPTunnelSetHelper::ToFineGrainedPacketV0(
            header,
            scattered,
            bucket_idx,
            fine_grained_shuffle_stream_count,
            num_columns,
            result_field_types);
        checker(tracked_packet, part_id);
    }

    static void write(tipb::SelectResponse &) { FAIL() << "cannot reach here, only consider CH Block format"; }
    uint16_t getPartitionNum() const { return part_num; }
    bool isLocal(size_t index) const
    {
        assert(getPartitionNum() > index);
        // make only part 0 use local tunnel
        return index == 0;
    }
    bool isReadyForWrite() const { throw Exception("Unsupport async write"); }

private:
    MockExchangeWriterChecker checker;
    uint16_t part_num;
    std::vector<tipb::FieldType> result_field_types;
};

// Input block data is distributed uniform.
// partition_num: 4
// fine_grained_shuffle_stream_count: 8
TEST_F(TestMPPExchangeWriter, testBatchWriteFineGrainedShuffle)
try
{
    const size_t block_rows = 1024;
    const uint16_t part_num = 4;
    const uint32_t fine_grained_shuffle_stream_count = 8;
    const Int64 fine_grained_shuffle_batch_size = 4096;

    // 1. Build Block.
    auto block = prepareUniformBlock(block_rows);

    // 2. Build MockExchangeWriter.
    std::unordered_map<uint16_t, TrackedMppDataPacketPtr> write_report;
    auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
        auto res = write_report.insert({part_id, packet});
        // Should always insert succeed.
        // Because block.rows(1024) < fine_grained_shuffle_batch_size(4096),
        // batchWriteFineGrainedShuffle() only called once, so will only be one packet for each partition.
        ASSERT_TRUE(res.second);
    };
    auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);

    // 3. Start to write.
    auto dag_writer = std::make_shared<FineGrainedShuffleWriter<std::shared_ptr<MockExchangeWriter>>>(
        mock_writer,
        part_col_ids,
        part_col_collators,
        *dag_context_ptr,
        fine_grained_shuffle_stream_count,
        fine_grained_shuffle_batch_size,
        DB::MPPDataPacketV0,
        tipb::CompressionMode::NONE);
    dag_writer->prepare(block.cloneEmpty());
    dag_writer->write(block);
    dag_writer->flush();

    // 4. Start to check write_report.
    std::vector<Block> decoded_blocks;
    ASSERT_EQ(write_report.size(), part_num);
    for (const auto & ele : write_report)
    {
        const TrackedMppDataPacketPtr & packet = ele.second;
        ASSERT_TRUE(packet);
        ASSERT_EQ(fine_grained_shuffle_stream_count, packet->getPacket().stream_ids_size());
        ASSERT_EQ(packet->getPacket().chunks_size(), packet->getPacket().stream_ids_size());
        for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
        {
            decoded_blocks.push_back(CHBlockChunkCodec::decode(packet->getPacket().chunks(i), block));
        }
    }
    ASSERT_EQ(decoded_blocks.size(), fine_grained_shuffle_stream_count * part_num);
    for (const auto & block : decoded_blocks)
    {
        ASSERT_EQ(block.rows(), block_rows / (fine_grained_shuffle_stream_count * part_num));
    }
}
CATCH

TEST_F(TestMPPExchangeWriter, TestFineGrainedShuffleWriterV1)
try
{
    const size_t block_rows = 64;
    const size_t block_num = 64;
    const uint16_t part_num = 4;
    const uint32_t fine_grained_shuffle_stream_count = 8;
    const Int64 fine_grained_shuffle_batch_size = 108;

    // 1. Build Block.
    std::vector<Block> blocks;
    for (size_t i = 0; i < block_num; ++i)
    {
        blocks.emplace_back(prepareUniformBlock(block_rows));
        blocks.emplace_back(prepareUniformBlock(0));
    }
    const auto & header = blocks.back().cloneEmpty();

    for (auto mode : {tipb::CompressionMode::NONE, tipb::CompressionMode::FAST, tipb::CompressionMode::HIGH_COMPRESSION})
    {
        // 2. Build MockExchangeWriter.
        std::unordered_map<uint16_t, TrackedMppDataPacketPtrs> write_report;
        auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
            write_report[part_id].emplace_back(packet);
        };
        auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);

        // 3. Start to write.
        auto dag_writer = std::make_shared<FineGrainedShuffleWriter<std::shared_ptr<MockExchangeWriter>>>(
            mock_writer,
            part_col_ids,
            part_col_collators,
            *dag_context_ptr,
            fine_grained_shuffle_stream_count,
            fine_grained_shuffle_batch_size,
            DB::MPPDataPacketV1,
            mode);
        dag_writer->prepare(blocks[0].cloneEmpty());
        for (const auto & block : blocks)
            dag_writer->write(block);
        dag_writer->flush();

        // 4. Start to check write_report.
        size_t per_part_rows = block_rows * block_num / part_num;
        ASSERT_EQ(write_report.size(), part_num);
        std::vector<size_t> rows_of_stream_ids(fine_grained_shuffle_stream_count, 0);

        CHBlockChunkDecodeAndSquash decoder(header, 512);

        for (size_t part_index = 0; part_index < part_num; ++part_index)
        {
            size_t part_decoded_block_rows = 0;

            for (const auto & packet : write_report[part_index])
            {
                ASSERT_EQ(packet->getPacket().chunks_size(), packet->getPacket().stream_ids_size());
                ASSERT_EQ(DB::MPPDataPacketV1, packet->getPacket().version());

                for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
                {
                    const auto & chunk = packet->getPacket().chunks(i);

                    auto tar_method_byte = mock_writer->isLocal(part_index) ? CompressionMethodByte::NONE : GetCompressionMethodByte(ToInternalCompressionMethod(mode));

                    ASSERT_EQ(CompressionMethodByte(chunk[0]), tar_method_byte);
                    auto && result = decoder.decodeAndSquashV1(chunk);
                    if (!result)
                    {
                        result = decoder.flush();
                    }
                    assert(result);
                    auto decoded_block = std::move(*result);
                    part_decoded_block_rows += decoded_block.rows();
                    rows_of_stream_ids[packet->getPacket().stream_ids(i)] += decoded_block.rows();
                }
            }
            ASSERT_EQ(part_decoded_block_rows, per_part_rows);
        }

        size_t per_stream_id_rows = block_rows * block_num / fine_grained_shuffle_stream_count;
        for (size_t rows : rows_of_stream_ids)
            ASSERT_EQ(rows, per_stream_id_rows);
    }
}
CATCH

TEST_F(TestMPPExchangeWriter, testFineGrainedShuffleWriter)
try
{
    const size_t block_rows = 64;
    const size_t block_num = 64;
    const uint16_t part_num = 4;
    const uint32_t fine_grained_shuffle_stream_count = 8;
    const Int64 fine_grained_shuffle_batch_size = 108;

    // 1. Build Block.
    std::vector<Block> blocks;
    for (size_t i = 0; i < block_num; ++i)
    {
        blocks.emplace_back(prepareUniformBlock(block_rows));
        blocks.emplace_back(prepareUniformBlock(0));
    }
    Block header = blocks.back();

    // 2. Build MockExchangeWriter.
    std::unordered_map<uint16_t, TrackedMppDataPacketPtrs> write_report;
    auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
        write_report[part_id].emplace_back(packet);
    };
    auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);

    // 3. Start to write.
    auto dag_writer = std::make_shared<FineGrainedShuffleWriter<std::shared_ptr<MockExchangeWriter>>>(
        mock_writer,
        part_col_ids,
        part_col_collators,
        *dag_context_ptr,
        fine_grained_shuffle_stream_count,
        fine_grained_shuffle_batch_size,
        DB::MPPDataPacketV0,
        tipb::CompressionMode::NONE);
    dag_writer->prepare(blocks[0].cloneEmpty());
    for (const auto & block : blocks)
        dag_writer->write(block);
    dag_writer->flush();

    // 4. Start to check write_report.
    size_t per_part_rows = block_rows * block_num / part_num;
    ASSERT_EQ(write_report.size(), part_num);
    std::vector<size_t> rows_of_stream_ids(fine_grained_shuffle_stream_count, 0);
    for (const auto & ele : write_report)
    {
        size_t part_decoded_block_rows = 0;
        for (const auto & packet : ele.second)
        {
            ASSERT_EQ(packet->getPacket().chunks_size(), packet->getPacket().stream_ids_size());
            for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
            {
                auto decoded_block = CHBlockChunkCodec::decode(packet->getPacket().chunks(i), header);
                part_decoded_block_rows += decoded_block.rows();
                rows_of_stream_ids[packet->getPacket().stream_ids(i)] += decoded_block.rows();
            }
        }
        ASSERT_EQ(part_decoded_block_rows, per_part_rows);
    }
    size_t per_stream_id_rows = block_rows * block_num / fine_grained_shuffle_stream_count;
    for (size_t rows : rows_of_stream_ids)
        ASSERT_EQ(rows, per_stream_id_rows);
}
CATCH

TEST_F(TestMPPExchangeWriter, testHashPartitionWriter)
try
{
    const size_t block_rows = 64;
    const size_t block_num = 64;
    const size_t batch_send_min_limit = 108;
    const uint16_t part_num = 4;

    // 1. Build Blocks.
    std::vector<Block> blocks;
    for (size_t i = 0; i < block_num; ++i)
    {
        blocks.emplace_back(prepareUniformBlock(block_rows));
        blocks.emplace_back(prepareUniformBlock(0));
    }
    Block header = blocks.back();

    // 2. Build MockExchangeWriter.
    std::unordered_map<uint16_t, TrackedMppDataPacketPtrs> write_report;
    auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
        write_report[part_id].emplace_back(packet);
    };
    auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);

    // 3. Start to write.
    auto dag_writer = std::make_shared<HashPartitionWriter<std::shared_ptr<MockExchangeWriter>>>(
        mock_writer,
        part_col_ids,
        part_col_collators,
        batch_send_min_limit,
        *dag_context_ptr,
        DB::MPPDataPacketV0,
        tipb::CompressionMode::NONE);
    for (const auto & block : blocks)
        dag_writer->write(block);
    dag_writer->flush();

    // 4. Start to check write_report.
    size_t per_part_rows = block_rows * block_num / part_num;
    ASSERT_EQ(write_report.size(), part_num);
    for (const auto & ele : write_report)
    {
        size_t decoded_block_rows = 0;
        for (const auto & packet : ele.second)
        {
            for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
            {
                auto decoded_block = CHBlockChunkCodec::decode(packet->getPacket().chunks(i), header);
                decoded_block_rows += decoded_block.rows();
            }
        }
        ASSERT_EQ(decoded_block_rows, per_part_rows);
    }
}
CATCH

TEST_F(TestMPPExchangeWriter, testBroadcastOrPassThroughWriter)
try
{
    const size_t block_rows = 64;
    const size_t block_num = 64;
    const size_t batch_send_min_limit = 108;

    // 1. Build Blocks.
    std::vector<Block> blocks;
    for (size_t i = 0; i < block_num; ++i)
    {
        blocks.emplace_back(prepareRandomBlock(block_rows));
        blocks.emplace_back(prepareRandomBlock(0));
    }
    Block header = blocks.back();

    // 2. Build MockExchangeWriter.
    TrackedMppDataPacketPtrs write_report;
    auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
        ASSERT_EQ(part_id, 0);
        write_report.emplace_back(packet);
    };
    auto mock_writer = std::make_shared<MockExchangeWriter>(checker, 1, *dag_context_ptr);

    // 3. Start to write.
    auto dag_writer = std::make_shared<BroadcastOrPassThroughWriter<std::shared_ptr<MockExchangeWriter>>>(
        mock_writer,
        batch_send_min_limit,
        *dag_context_ptr,
        MPPDataPacketVersion::MPPDataPacketV0,
        tipb::CompressionMode::NONE,
        tipb::ExchangeType::Broadcast);

    for (const auto & block : blocks)
        dag_writer->write(block);
    dag_writer->flush();

    // 4. Start to check write_report.
    size_t expect_rows = block_rows * block_num;
    size_t decoded_block_rows = 0;
    for (const auto & packet : write_report)
    {
        for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
        {
            auto decoded_block = CHBlockChunkCodec::decode(packet->getPacket().chunks(i), header);
            decoded_block_rows += decoded_block.rows();
        }
    }
    ASSERT_EQ(decoded_block_rows, expect_rows);
}
CATCH

TEST_F(TestMPPExchangeWriter, TestBroadcastOrPassThroughWriterV1)
try
{
    const size_t block_rows = 64;
    const size_t block_num = 64;
    const size_t batch_send_min_limit = 108;

    // 1. Build Blocks.
    std::vector<Block> blocks;
    for (size_t i = 0; i < block_num; ++i)
    {
        blocks.emplace_back(prepareRandomBlock(block_rows));
        blocks.emplace_back(prepareRandomBlock(0));
    }
    Block header = blocks.back();
    for (auto mode : {tipb::CompressionMode::NONE, tipb::CompressionMode::FAST, tipb::CompressionMode::HIGH_COMPRESSION})
    {
        // 2. Build MockExchangeWriter.
        TrackedMppDataPacketPtrs write_report;
        auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
            ASSERT_EQ(part_id, 0);
            write_report.emplace_back(packet);
        };
        auto mock_writer = std::make_shared<MockExchangeWriter>(checker, 1, *dag_context_ptr);

        // 3. Start to write.
        auto dag_writer = std::make_shared<BroadcastOrPassThroughWriter<std::shared_ptr<MockExchangeWriter>>>(
            mock_writer,
            batch_send_min_limit,
            *dag_context_ptr,
            MPPDataPacketVersion::MPPDataPacketV1,
            mode,
            tipb::ExchangeType::Broadcast);

        for (const auto & block : blocks)
            dag_writer->write(block);
        dag_writer->flush();

        // 4. Start to check write_report.
        size_t expect_rows = block_rows * block_num;
        size_t decoded_block_rows = 0;
        for (const auto & packet : write_report)
        {
            for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
            {
                auto decoded_block = CHBlockChunkCodecV1::decode(header, packet->getPacket().chunks(i));
                decoded_block_rows += decoded_block.rows();
            }
        }
        ASSERT_EQ(decoded_block_rows, expect_rows);
    }
}
CATCH

static CompressionMethodByte GetCompressionMethodByte(CompressionMethod m)
{
    switch (m)
    {
    case CompressionMethod::LZ4:
        return CompressionMethodByte::LZ4;
    case CompressionMethod::NONE:
        return CompressionMethodByte::NONE;
    case CompressionMethod::ZSTD:
        return CompressionMethodByte::ZSTD;
    default:
        RUNTIME_CHECK(false);
    }
    return CompressionMethodByte::NONE;
}

TEST_F(TestMPPExchangeWriter, TestHashPartitionWriterV1)
try
{
    const size_t block_rows = 64;
    const size_t block_num = 64;
    const size_t batch_send_min_limit = 1024 * 1024 * 1024;
    const uint16_t part_num = 4;

    // 1. Build Blocks.
    std::vector<Block> blocks;
    for (size_t i = 0; i < block_num; ++i)
    {
        blocks.emplace_back(prepareUniformBlock(block_rows));
        blocks.emplace_back(prepareUniformBlock(0));
    }
    const auto & header = blocks.back().cloneEmpty();

    for (auto mode : {tipb::CompressionMode::NONE, tipb::CompressionMode::FAST, tipb::CompressionMode::HIGH_COMPRESSION})
    {
        // 2. Build MockExchangeWriter.
        std::unordered_map<uint16_t, TrackedMppDataPacketPtrs> write_report;
        auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
            write_report[part_id].emplace_back(packet);
        };
        auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);

        // 3. Start to write.
        auto dag_writer = std::make_shared<HashPartitionWriter<std::shared_ptr<MockExchangeWriter>>>(
            mock_writer,
            part_col_ids,
            part_col_collators,
            batch_send_min_limit,
            *dag_context_ptr,
            DB::MPPDataPacketV1,
            mode);
        for (const auto & block : blocks)
            dag_writer->write(block);
        dag_writer->write(header); // write empty
        dag_writer->flush();

        // 4. Start to check write_report.
        size_t per_part_rows = block_rows * block_num / part_num;
        ASSERT_EQ(write_report.size(), part_num);

        CHBlockChunkDecodeAndSquash decoder(header, 512);

        for (size_t part_index = 0; part_index < part_num; ++part_index)
        {
            size_t decoded_block_rows = 0;
            for (const auto & tracked_packet : write_report[part_index])
            {
                auto & packet = tracked_packet->getPacket();

                ASSERT_EQ(packet.version(), DB::MPPDataPacketV1);

                for (auto && chunk : packet.chunks())
                {
                    auto tar_method_byte = mock_writer->isLocal(part_index) ? CompressionMethodByte::NONE : GetCompressionMethodByte(ToInternalCompressionMethod(mode));
                    ASSERT_EQ(CompressionMethodByte(chunk[0]), tar_method_byte);
                    auto && result = decoder.decodeAndSquashV1(chunk);
                    if (!result)
                        continue;
                    decoded_block_rows += result->rows();
                }
            }
            {
                auto result = decoder.flush();
                if (result)
                    decoded_block_rows += result->rows();
            }
            ASSERT_EQ(decoded_block_rows, per_part_rows);
        }
    }
}
CATCH
} // namespace tests
} // namespace DB
