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
#include <Flash/Coprocessor/StreamingDAGResponseWriter.cpp>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>

#include <gtest/gtest.h>
#include <iostream>

namespace DB
{
namespace tests
{

using BlockPtr = std::shared_ptr<Block>;
class TestStreamingDAGResponseWriter : public testing::Test
{
    protected:
        void SetUp() override
        {
            dag_context_ptr = std::make_unique<DAGContext>(1024);
            dag_context_ptr->encode_type = tipb::EncodeType::TypeCHBlock;
            dag_context_ptr->is_mpp_task = true;
            dag_context_ptr->is_root_mpp_task = false;
            dag_context_ptr->result_field_types = makeFields();
            context.setDAGContext(dag_context_ptr.get());
        }
    public:
        TestStreamingDAGResponseWriter()
            : context(TiFlashTestEnv::getContext()),
              part_col_ids{0},
              part_col_collators{
                  TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY)
              } {}

        // Return 10 Int64 column.
        static std::vector<tipb::FieldType> makeFields()
        {
            std::vector<tipb::FieldType> fields(10);
            for (int i = 0; i < 10; ++i)
            {
                fields[i].set_tp(TiDB::TypeLongLong);
            }
            return fields;
        }

        // Return a block with **rows** and 10 Int64 column.
        static BlockPtr prepareBlock(const std::vector<Int64> & rows)
        {
            BlockPtr block = std::make_shared<Block>();
            for (int i = 0; i < 10; ++i)
            {
                DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
                DataTypePtr nullable_int64_data_type = std::make_shared<DataTypeNullable>(int64_data_type);
                MutableColumnPtr int64_col = nullable_int64_data_type->createColumn();
                for (Int64 r : rows)
                {
                    int64_col->insert(Field(r));
                }
                block->insert(ColumnWithTypeAndName{std::move(int64_col),
                        nullable_int64_data_type,
                        String("col") + std::to_string(i)});
            }
            return block;
        }

        Context context;
        std::vector<Int64> part_col_ids;
        TiDB::TiDBCollators part_col_collators;

        std::unique_ptr<DAGContext> dag_context_ptr;
};

using MockStreamWriterChecker = std::function<void(mpp::MPPDataPacket &, uint16_t)>;

struct MockStreamWriter
{
    MockStreamWriter(MockStreamWriterChecker checker_,
            uint16_t part_num_)
        : checker(checker_)
        , part_num(part_num_) {}

    void write(mpp::MPPDataPacket &) { FAIL() << "cannot reach here, because we only expect hash partition"; }
    void write(mpp::MPPDataPacket & packet, uint16_t part_id) { checker(packet, part_id); }
    void write(tipb::SelectResponse &, uint16_t) { FAIL() << "cannot reach here, only consider CH Block format"; }
    void write(tipb::SelectResponse &) { FAIL() << "cannot reach here, only consider CH Block format"; }
    uint16_t getPartitionNum() const { return part_num; }

    private:
    MockStreamWriterChecker checker;
    uint16_t part_num;
};

// Input block data is distributed uniform.
// partition_num: 4
// fine_grained_shuffle_stream_count: 8
TEST_F(TestStreamingDAGResponseWriter, testBatchWriteFineGrainedShuffle)
try
{
    const size_t block_rows = 1024;
    const uint16_t part_num = 4;
    const uint32_t fine_grained_shuffle_stream_count = 8;
    const Int64 fine_grained_shuffle_batch_size = 4096;
    dag_context_ptr->setFineGrainedShuffleStreamCount(fine_grained_shuffle_stream_count);
    dag_context_ptr->setFineGrainedShuffleBatchSize(fine_grained_shuffle_batch_size);
    
    // Set these to 1, because when fine grained shuffle is enabled, 
    // batchWriteFineGrainedShuffle() only check fine_grained_shuffle_batch_size.
    // records_per_chunk and batch_send_min_limit are useless.
    const Int64 records_per_chunk = 1;
    const Int64 batch_send_min_limit = 1;
    const bool should_send_exec_summary_at_last = true;

    // 1. Build Block.
    std::vector<Int64> uniform_data_set;
    for (size_t i = 0; i < block_rows; ++i)
    {
        uniform_data_set.push_back(i);
    }
    BlockPtr block = prepareBlock(uniform_data_set);

    // 2. Build MockStreamWriter.
    std::unordered_map<uint16_t, mpp::MPPDataPacket> write_report;
    auto checker = [&write_report](mpp::MPPDataPacket & packet, uint16_t part_id) {
        auto res = write_report.insert({part_id, packet});
        // Should always insert succeed.
        // Because block.rows(1024) < fine_grained_shuffle_batch_size(4096),
        // batchWriteFineGrainedShuffle() only called once, so will only be one packet for each partition.
        ASSERT_TRUE(res.second);
    };
    auto mock_writer = std::make_shared<MockStreamWriter>(checker, part_num);

    // 3. Start to write.
    auto dag_writer = std::make_shared<StreamingDAGResponseWriter<std::shared_ptr<MockStreamWriter>>>(
            mock_writer,
            part_col_ids,
            part_col_collators, 
            tipb::ExchangeType::Hash,
            records_per_chunk,
            batch_send_min_limit,
            should_send_exec_summary_at_last,
            *dag_context_ptr,
            fine_grained_shuffle_stream_count,
            fine_grained_shuffle_batch_size);
    dag_writer->write(*block);
    dag_writer->finishWrite();

    // 4. Start to check write_report.
    std::vector<Block> decoded_blocks;
    ASSERT_EQ(write_report.size(), part_num);
    for (const auto & ele : write_report)
    {
        const mpp::MPPDataPacket & packet = ele.second;
        ASSERT_EQ(packet.chunks_size(), packet.stream_ids_size());
        for (int i = 0; i < packet.chunks_size(); ++i)
        {
            decoded_blocks.push_back(CHBlockChunkCodec::decode(packet.chunks(i), *block));
        }
    }
    ASSERT_EQ(decoded_blocks.size(), fine_grained_shuffle_stream_count * part_num);
    for (const auto & block : decoded_blocks)
    {
        ASSERT_EQ(block.rows(), block_rows / (fine_grained_shuffle_stream_count * part_num));
    }
}
CATCH

} // namespace tests
} // namespace DB
