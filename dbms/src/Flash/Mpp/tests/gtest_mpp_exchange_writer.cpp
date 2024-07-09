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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnTuple.h>
#include <Common/Logger.h>
#include <Common/RandomData.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTunnelSetHelper.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Schema/TiDB.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>

#include <Flash/Mpp/BroadcastOrPassThroughWriter.cpp>
#include <Flash/Mpp/FineGrainedShuffleWriter.cpp>
#include <Flash/Mpp/HashPartitionWriter.cpp>
#include <cstdlib>
#include <ctime>

namespace DB
{
namespace tests
{
static CompressionMethodByte GetCompressionMethodByte(CompressionMethod m);

using MockExchangeWriterChecker = std::function<void(const TrackedMppDataPacketPtr &, uint16_t)>;

struct MockExchangeWriter
{
    MockExchangeWriter(MockExchangeWriterChecker checker_, uint16_t part_num_, DAGContext & dag_context)
        : checker(checker_)
        , part_num(part_num_)
        , result_field_types(dag_context.result_field_types)
        , gen(rd())
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
        auto tracked_packet
            = MPPTunnelSetHelper::ToPacket(header, std::move(part_columns), version, method, original_size);
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
            return fineGrainedShuffleWrite(
                header,
                scattered,
                bucket_idx,
                fine_grained_shuffle_stream_count,
                num_columns,
                part_id);
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

    void broadcastWrite(Blocks & blocks) { return broadcastOrPassThroughWriteV0(blocks); }
    void passThroughWrite(Blocks & blocks) { return broadcastOrPassThroughWriteV0(blocks); }
    void broadcastOrPassThroughWrite(
        Blocks & blocks,
        MPPDataPacketVersion version,
        CompressionMethod compression_method)
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
    bool isWritable() const { throw Exception("Unsupport async write"); }

private:
    MockExchangeWriterChecker checker;
    uint16_t part_num;
    std::vector<tipb::FieldType> result_field_types;
    std::random_device rd;
    std::mt19937_64 gen;
};

class TestMPPExchangeWriter : public testing::Test
{
protected:
    void SetUp() override
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        dag_context_ptr->encode_type = tipb::EncodeType::TypeCHBlock;
        dag_context_ptr->kind = DAGRequestKind::MPP;
        dag_context_ptr->is_root_mpp_task = false;
        dag_context_ptr->result_field_types = makeFields();
    }

public:
    TestMPPExchangeWriter()
        : part_col_ids{0}
        , part_col_collators{TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY)}
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
            block.insert(
                ColumnWithTypeAndName{std::move(int64_col), int64_data_type, String("col") + std::to_string(i)});
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
            block.insert(
                ColumnWithTypeAndName{std::move(int64_column), int64_data_type, String("col") + std::to_string(i)});
        }
        return block;
    }

    static std::vector<std::vector<Block>> decodeWriteReportsV0(
        const Block & header,
        const std::unordered_map<uint16_t, TrackedMppDataPacketPtr> & write_reports)
    {
        std::vector<std::vector<Block>> decoded_blocks;
        decoded_blocks.reserve(write_reports.size());
        for (const auto & rec : write_reports)
        {
            const TrackedMppDataPacketPtr & packet = rec.second;
            std::vector<Block> part_blocks;
            part_blocks.reserve(packet->getPacket().chunks_size());
            for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
            {
                part_blocks.push_back(CHBlockChunkCodec::decode(packet->getPacket().chunks(i), header));
            }
            decoded_blocks.push_back(part_blocks);
        }
        return decoded_blocks;
    }

    static std::vector<std::vector<Block>> decodeWriteReportsV1(
        bool is_fine_grained_shuffle,
        const tipb::CompressionMode & mode,
        const std::shared_ptr<MockExchangeWriter> & mock_writer,
        const Block & header,
        const std::unordered_map<uint16_t, TrackedMppDataPacketPtr> & write_reports)
    {
        CHBlockChunkDecodeAndSquash decoder(header, 512);
        std::vector<std::vector<Block>> decoded_blocks;

        for (const auto & report : write_reports)
        {
            const auto part_index = report.first;
            const TrackedMppDataPacketPtr & packet = report.second;

            if unlikely (
                is_fine_grained_shuffle && packet->getPacket().chunks_size() != packet->getPacket().stream_ids_size())
                throw Exception("unexpected stream id size");

            if unlikely (DB::MPPDataPacketV1 != packet->getPacket().version())
                throw Exception("expect mpp version V1");

            std::vector<Block> part_blocks;
            part_blocks.reserve(packet->getPacket().chunks_size());

            for (int i = 0; i < packet->getPacket().chunks_size(); ++i)
            {
                const auto & chunk = packet->getPacket().chunks(i);

                auto tar_method_byte = mock_writer->isLocal(part_index)
                    ? CompressionMethodByte::NONE
                    : GetCompressionMethodByte(ToInternalCompressionMethod(mode));

                if unlikely (CompressionMethodByte(chunk[0]) != tar_method_byte)
                    throw Exception("unexpected tar_method_byte");

                auto && result = decoder.decodeAndSquashV1(chunk);
                if (!result)
                    result = decoder.flush();
                assert(result);
                part_blocks.push_back(std::move(*result));
            }
            decoded_blocks.push_back(part_blocks);
        }
        return decoded_blocks;
    }

    static std::vector<UInt64> generateRandomSelective(size_t block_total_rows, size_t selective_rows)
    {
        if unlikely (block_total_rows < selective_rows)
            throw Exception("expect total rows is greater than selective rows");

        std::vector<UInt64> selective(selective_rows, 0);
        std::set<UInt64> selective_dup;

        for (auto & selected_row : selective)
        {
            UInt64 rand_row = rand() % block_total_rows; // NOLINT
            for (size_t j = 0; j < block_total_rows; ++j)
            {
                if (selective_dup.find(rand_row) == selective_dup.end())
                {
                    break;
                }
                else
                {
                    ++rand_row;
                }
            }
            if unlikely (selective_dup.find(rand_row) != selective_dup.end())
                throw Exception("expect random_row is unique");

            selective_dup.insert(rand_row);
            selected_row = rand_row;
        }
        if unlikely (selective_dup.size() != selective.size())
            throw Exception("unexpected selective vec size");
        return selective;
    }

    static void decodeAndCheckBlockForSelectiveBlock(
        bool is_fine_grained_shuffle,
        const DB::MPPDataPacketVersion & mpp_version,
        const Block & header,
        size_t expect_part_num,
        size_t expect_selective_rows,
        tipb::CompressionMode mode,
        std::shared_ptr<MockExchangeWriter> mock_writer,
        const std::unordered_map<uint16_t, TrackedMppDataPacketPtr> & write_records)
    {
        std::vector<std::vector<Block>> decoded_blocks;
        ASSERT_EQ(write_records.size(), expect_part_num);
        if (mpp_version == DB::MPPDataPacketV0)
            decoded_blocks = decodeWriteReportsV0(header, write_records);
        else
            decoded_blocks = decodeWriteReportsV1(is_fine_grained_shuffle, mode, mock_writer, header, write_records);

        size_t res_rows = 0;
        for (const auto & part_blocks : decoded_blocks)
        {
            for (const auto & block : part_blocks)
            {
                res_rows += block.rows();
            }
        }
        // There should be part_num packet.
        ASSERT_EQ(decoded_blocks.size(), expect_part_num);
        // Total sent rows should equals selective count.
        ASSERT_EQ(res_rows, expect_selective_rows);
    }

    std::vector<Int64> part_col_ids;
    TiDB::TiDBCollators part_col_collators;

    std::unique_ptr<DAGContext> dag_context_ptr;
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
    auto dag_writer = std::make_shared<FineGrainedShuffleWriter<std::shared_ptr<MockExchangeWriter>, false>>(
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
    // todo use decodeWriteReportsV0
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

TEST_F(TestMPPExchangeWriter, testFineGrainedShuffleWriterV1)
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

    for (auto mode :
         {tipb::CompressionMode::NONE, tipb::CompressionMode::FAST, tipb::CompressionMode::HIGH_COMPRESSION})
    {
        // 2. Build MockExchangeWriter.
        std::unordered_map<uint16_t, TrackedMppDataPacketPtrs> write_report;
        auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
            write_report[part_id].emplace_back(packet);
        };
        auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);

        // 3. Start to write.
        auto dag_writer = std::make_shared<FineGrainedShuffleWriter<std::shared_ptr<MockExchangeWriter>, false>>(
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

        // todo use decodeWriteReportsV1 instead
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

                    auto tar_method_byte = mock_writer->isLocal(part_index)
                        ? CompressionMethodByte::NONE
                        : GetCompressionMethodByte(ToInternalCompressionMethod(mode));

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
    auto dag_writer = std::make_shared<FineGrainedShuffleWriter<std::shared_ptr<MockExchangeWriter>, false>>(
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
    auto dag_writer = std::make_shared<HashPartitionWriter<std::shared_ptr<MockExchangeWriter>, false>>(
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

TEST_F(TestMPPExchangeWriter, testBroadcastOrPassThroughWriterV1)
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
    for (auto mode :
         {tipb::CompressionMode::NONE, tipb::CompressionMode::FAST, tipb::CompressionMode::HIGH_COMPRESSION})
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

TEST_F(TestMPPExchangeWriter, testHashPartitionWriterV1)
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

    for (auto mode :
         {tipb::CompressionMode::NONE, tipb::CompressionMode::FAST, tipb::CompressionMode::HIGH_COMPRESSION})
    {
        // 2. Build MockExchangeWriter.
        std::unordered_map<uint16_t, TrackedMppDataPacketPtrs> write_report;
        auto checker = [&write_report](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
            write_report[part_id].emplace_back(packet);
        };
        auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);

        // 3. Start to write.
        auto dag_writer = std::make_shared<HashPartitionWriter<std::shared_ptr<MockExchangeWriter>, false>>(
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
                    auto tar_method_byte = mock_writer->isLocal(part_index)
                        ? CompressionMethodByte::NONE
                        : GetCompressionMethodByte(ToInternalCompressionMethod(mode));
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

// FineGrainedShuffleWriter writes one selective block(with BlockInfo.selective not null).
// Check only write selected rows.
TEST_F(TestMPPExchangeWriter, testFineGrainedShuffleWriterWithSelectiveBlock)
try
{
    // Two tiflash instance(a.k.a. two partition)
    const size_t part_num = 2;
    const size_t fine_grained_shuffle_stream_count = 4;
    const size_t fine_grained_shuffle_batch_size = 1024;
    const size_t block_total_rows = 4096;
    const size_t selective_rows = 128;

    // Construct block with N selected rows.
    auto block = prepareUniformBlock(block_total_rows);
    auto selective = generateRandomSelective(block_total_rows, selective_rows);
    block.info.selective = std::make_shared<std::vector<UInt64>>(std::move(selective));

    std::vector<MPPDataPacketVersion> mpp_version_vec{DB::MPPDataPacketV0, DB::MPPDataPacketV1};

    for (const auto & mpp_version : mpp_version_vec)
    {
        // Construct checker.
        std::unordered_map<uint16_t, TrackedMppDataPacketPtr> write_records;
        MockExchangeWriterChecker checker = [&](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
            auto res = write_records.insert({part_id, packet});
            ASSERT_TRUE(res.second);
        };

        auto mode = tipb::CompressionMode::NONE;
        if (mpp_version == DB::MPPDataPacketV1)
            mode = tipb::CompressionMode::FAST;

        // Construct dag_writer.
        auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);
        auto dag_writer = std::make_shared<FineGrainedShuffleWriter<std::shared_ptr<MockExchangeWriter>, true>>(
            mock_writer,
            part_col_ids,
            part_col_collators,
            *dag_context_ptr,
            fine_grained_shuffle_stream_count,
            fine_grained_shuffle_batch_size,
            mpp_version,
            mode);

        // Write selective block.
        dag_writer->prepare(block.cloneEmpty());
        dag_writer->write(block);
        dag_writer->flush();

        // Check block.
        decodeAndCheckBlockForSelectiveBlock(
            /*is_fine_grained_shuffle*/ true,
            mpp_version,
            block,
            part_num,
            selective_rows,
            mode,
            mock_writer,
            write_records);
    }
}
CATCH

TEST_F(TestMPPExchangeWriter, testHashPartitionWriterWithSelectiveBlock)
try
{
    // Two tiflash instance(a.k.a. two partition)
    const size_t part_num = 2;
    const size_t block_total_rows = 4096;
    const size_t selective_rows = 128;

    // Construct block with N selected rows.
    auto block = prepareUniformBlock(block_total_rows);
    auto selective = generateRandomSelective(block_total_rows, selective_rows);
    block.info.selective = std::make_shared<std::vector<UInt64>>(std::move(selective));

    std::vector<MPPDataPacketVersion> mpp_version_vec{DB::MPPDataPacketV0, DB::MPPDataPacketV1};

    for (const auto & mpp_version : mpp_version_vec)
    {
        // Construct checker.
        std::unordered_map<uint16_t, TrackedMppDataPacketPtr> write_records;
        MockExchangeWriterChecker checker = [&](const TrackedMppDataPacketPtr & packet, uint16_t part_id) {
            auto res = write_records.insert({part_id, packet});
            ASSERT_TRUE(res.second);
        };

        auto mode = tipb::CompressionMode::NONE;
        if (mpp_version == DB::MPPDataPacketV1)
            mode = tipb::CompressionMode::FAST;

        // Construct dag_writer.
        auto mock_writer = std::make_shared<MockExchangeWriter>(checker, part_num, *dag_context_ptr);
        auto dag_writer = std::make_shared<HashPartitionWriter<std::shared_ptr<MockExchangeWriter>, true>>(
            mock_writer,
            part_col_ids,
            part_col_collators,
            /*batch_send_min_limit*/ 1024,
            *dag_context_ptr,
            mpp_version,
            mode);

        // Write selective block.
        // dag_writer->prepare(block.cloneEmpty());
        dag_writer->write(block);
        dag_writer->flush();

        // Check block.
        decodeAndCheckBlockForSelectiveBlock(
            /*is_fine_grained_shuffle*/ false,
            mpp_version,
            block,
            part_num,
            selective_rows,
            mode,
            mock_writer,
            write_records);
    }
}
CATCH

TEST_F(TestMPPExchangeWriter, testSelectiveBlockUpdateWeakHash32)
try
{
    String sort_key_container;
    const size_t rows = 65535;
    const size_t selective_rows = 1024;
    auto collators = std::vector<TiDB::TiDBCollatorPtr>{
        TiDB::ITiDBCollator::getCollator("utf8mb4_bin"),
        TiDB::ITiDBCollator::getCollator("binary"),
        TiDB::ITiDBCollator::getCollator("utf8mb4_general_ci"),
    };

    // One column will be scattered to N columns.
    const IColumn::ColumnIndex num_columns = 4;

    auto update_hash_checker = [&](ColumnWithTypeAndName & column) {
        LOG_DEBUG(Logger::get(), "TestMPPExchangeWriter.testSelectiveBlockUpdateWeakHash32 checking {}", column.name);
        for (auto & collator : collators)
        {
            WeakHash32 hash_no_selective(rows);
            column.column->updateWeakHash32(hash_no_selective, collator, sort_key_container);

            auto selective = std::make_shared<std::vector<UInt64>>(generateRandomSelective(rows, selective_rows));
            WeakHash32 hash_selective_block(selective->size());
            column.column->updateWeakHash32(hash_selective_block, collator, sort_key_container, selective);

            ASSERT_EQ(selective->size(), hash_selective_block.getData().size());
            for (size_t i = 0; i < selective->size(); ++i)
                ASSERT_EQ(hash_no_selective.getData()[(*selective)[i]], hash_selective_block.getData()[i]);
        }
    };

    auto scatter_checker = [&](ColumnWithTypeAndName & column) {
        LOG_DEBUG(Logger::get(), "TestMPPExchangeWriter.testSelectiveBlockScatter checking {}", column.name);
        IColumn::Selector selector;
        selector.reserve(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            selector.push_back(rand() % num_columns); // NOLINT
        }

        auto scatter_columns_no_selective_block = column.column->scatter(num_columns, selector);

        auto selective = generateRandomSelective(rows, selective_rows);
        IColumn::Selector selector_selective;
        selector_selective.reserve(selective_rows);
        for (size_t i = 0; i < selective_rows; ++i)
        {
            selector_selective.push_back(selector[selective[i]]);
        }
        auto scatter_columns_selective_block
            = column.column->scatter(num_columns, selector_selective, std::make_shared<std::vector<UInt64>>(selective));

        size_t rows_after_scatter = 0;
        for (const auto & col : scatter_columns_selective_block)
        {
            rows_after_scatter += col->size();
        }

        LOG_DEBUG(Logger::get(), "TestMPPExchangeWriter.testSelectiveBlockScatter checking {} assert beg", column.name);
        ASSERT_EQ(selective.size(), rows_after_scatter);
        ASSERT_EQ(scatter_columns_no_selective_block.size(), scatter_columns_selective_block.size());

        for (size_t i = 0; i < scatter_columns_selective_block.size(); ++i)
        {
            size_t selector_rows = 0;
            for (auto j : selective)
                if (selector[j] == i)
                    ++selector_rows;

            ASSERT_EQ(selector_rows, scatter_columns_selective_block[i]->size());
        }

        if (column.name == "col_nothing")
            return;

        for (size_t i = 0; i < scatter_columns_selective_block.size(); ++i)
        {
            const auto & col_selective = scatter_columns_selective_block[i];
            const auto & col_no_selective = scatter_columns_no_selective_block[i];
            for (size_t x = 0; x < col_selective->size(); ++x)
            {
                bool found = false;
                for (size_t y = 0; y < col_no_selective->size(); ++y)
                {
                    if ((*col_selective)[x] == (*col_no_selective)[y])
                        found = true;
                }
                ASSERT_TRUE(found);
            }
        }
        LOG_DEBUG(
            Logger::get(),
            "TestMPPExchangeWriter.testSelectiveBlockScatter checking {} assert done",
            column.name);
    };

    // ColumnAggregateFunction
    ::DB::registerAggregateFunctions();
    auto data_type_int64 = std::make_shared<DataTypeInt64>();
    auto context = TiFlashTestEnv::getContext();
    auto agg_func = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_int64});
    auto col_agg_func = ColumnAggregateFunction::create(agg_func);
    for (size_t i = 0; i < rows; ++i)
    {
        String tmp = ::DB::random::randomString(sizeof(UInt64));
        col_agg_func->insert(Field(tmp.data(), tmp.size()));
    }
    DataTypes argument_types{data_type_int64};
    Array params_row;
    ColumnWithTypeAndName col_agg_func_with_name{
        std::move(col_agg_func),
        std::make_shared<DataTypeAggregateFunction>(agg_func, argument_types, params_row),
        std::string("col_agg_func")};
    update_hash_checker(col_agg_func_with_name);
    scatter_checker(col_agg_func_with_name);

    // ColumnInt64
    auto col_int64 = ColumnGenerator::instance().generate({rows, data_type_int64->getName(), RANDOM, "col_int64"});
    update_hash_checker(col_int64);
    scatter_checker(col_int64);

    // ColumnString
    auto data_type_str = std::make_shared<DataTypeString>();
    auto col_string = ColumnGenerator::instance().generate({rows, data_type_str->getName(), RANDOM, "col_string"});
    update_hash_checker(col_string);
    scatter_checker(col_string);

    // ColumnFixedString
    size_t fixed_length = 128;
    auto data_type_fixed_str = std::make_shared<DataTypeFixedString>(fixed_length);
    auto col_fixed_string = ColumnGenerator::instance().generate(
        {.size = rows,
         .type_name = data_type_fixed_str->getName(),
         .distribution = DataDistribution::RANDOM,
         .name = "col_fixed_string",
         .string_max_size = fixed_length});
    update_hash_checker(col_fixed_string);
    scatter_checker(col_fixed_string);

    // ColumnConst
    auto col_nested_str = data_type_str->createColumn();
    const String const_str = "abc";
    col_nested_str->insert(Field(const_str.data(), const_str.size()));
    auto col_const = ColumnConst::create(std::move(col_nested_str), rows);
    ColumnWithTypeAndName col_const_with_name{std::move(col_const), data_type_str, std::string("col_const")};
    update_hash_checker(col_const_with_name);
    scatter_checker(col_const_with_name);

    // ColumnDecimal
    auto data_type_decimal = std::make_shared<DataTypeDecimal64>();
    auto col_decimal
        = ColumnGenerator::instance().generate({rows, data_type_decimal->getName(), RANDOM, "col_decimal"});
    update_hash_checker(col_decimal);
    scatter_checker(col_decimal);

    // ColumnNullable
    auto col_string_1 = ColumnGenerator::instance().generate({rows, data_type_str->getName(), RANDOM});
    auto col_null_map = ColumnVector<UInt8>::create();
    srand(time(nullptr));
    for (size_t i = 0; i < rows; ++i)
    {
        auto v = rand() % 2; // NOLINT
        col_null_map->insert(Field(static_cast<UInt64>(v)));
    }
    auto col_nullable = ColumnNullable::create(col_string_1.column, std::move(col_null_map));
    ColumnWithTypeAndName col_nullable_with_name{
        col_nullable,
        std::make_shared<DataTypeNullable>(data_type_str),
        "col_nullable<col_string>"};
    update_hash_checker(col_nullable_with_name);
    scatter_checker(col_nullable_with_name);

    // ColumnTuple
    auto col_string_2 = ColumnGenerator::instance().generate({rows, data_type_str->getName(), RANDOM});
    auto col_int64_2 = ColumnGenerator::instance().generate({rows, data_type_int64->getName(), RANDOM});
    auto col_float_2 = ColumnGenerator::instance().generate({rows, DataTypeFloat64().getName(), RANDOM});
    Columns tuple_cols{col_string_2.column, col_int64_2.column, col_float_2.column};
    auto col_tuple = ColumnTuple::create(tuple_cols);
    ColumnWithTypeAndName col_tuple_with_name{
        std::move(col_tuple),
        std::make_shared<DataTypeTuple>(DataTypes{
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeFloat64>()}),
        "col_nullable<col_string, col_int64, col_float64>"};
    update_hash_checker(col_tuple_with_name);
    scatter_checker(col_tuple_with_name);

    // ColumnArray
    auto col_offset = ColumnUInt64::create();
    const size_t array_size = 4;
    for (size_t i = 1; i <= rows; ++i)
    {
        col_offset->insert(Field(static_cast<UInt64>(i * array_size)));
    }
    auto col_array_nested = ColumnGenerator::instance().generate({rows * array_size, data_type_str->getName(), RANDOM});
    auto col_array = ColumnArray::create(col_array_nested.column, std::move(col_offset));
    ColumnWithTypeAndName col_array_with_name{
        std::move(col_array),
        std::make_shared<DataTypeArray>(data_type_str),
        "col_array"};
    update_hash_checker(col_array_with_name);
    scatter_checker(col_string);

    // ColumnNothing
    auto col_nothing = ColumnNothing::create(0);
    for (size_t i = 0; i < rows; ++i)
        col_nothing->insertDefault();
    ColumnWithTypeAndName col_nothing_with_name{
        std::move(col_nothing),
        std::make_shared<DataTypeNothing>(),
        "col_nothing"};
    update_hash_checker(col_nothing_with_name);
    scatter_checker(col_nothing_with_name);

    // ColumnFunction: no need to test. Function impl will throw exception.
}
CATCH

} // namespace tests
} // namespace DB
