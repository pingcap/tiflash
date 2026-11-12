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

#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/UnaryDAGResponseWriter.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

#include <Flash/Coprocessor/StreamingDAGResponseWriter.cpp>
#include <cstddef>
#include <limits>

namespace DB
{
namespace tests
{
class TestStreamingWriter : public testing::Test
{
protected:
    void SetUp() override
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        dag_context_ptr->kind = DAGRequestKind::MPP;
        dag_context_ptr->is_root_mpp_task = true;
        dag_context_ptr->result_field_types = makeFields();
    }

public:
    TestStreamingWriter() = default;

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

    static DAGSchema makeSchema()
    {
        auto fields = makeFields();
        DAGSchema schema;
        for (size_t i = 0; i < fields.size(); ++i)
        {
            TiDB::ColumnInfo info = TiDB::fieldTypeToColumnInfo(fields[i]);
            schema.emplace_back(String("col") + std::to_string(i), std::move(info));
        }
        return schema;
    }

    // Return a block with **rows** and 10 Int64 column.
    static Block prepareBlock(size_t rows)
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

    std::unique_ptr<DAGContext> dag_context_ptr;
};

using MockStreamWriterChecker = std::function<void(tipb::SelectResponse &)>;

struct MockStreamWriter
{
    explicit MockStreamWriter(MockStreamWriterChecker checker_)
        : checker(checker_)
    {}

    void write(tipb::SelectResponse & response) { checker(response); }
    static WaitResult waitForWritable() { return WaitResult::Ready; }

private:
    MockStreamWriterChecker checker;
};

template <typename Writer>
class TestDAGWriter : public Writer
{
public:
    using Writer::Writer;

    // Stop a regression before write() can enter a loop that makes no progress.
    bool hasValidChunkSize() const { return this->records_per_chunk > 0 || this->records_per_chunk == -1; }
};

TEST_F(TestStreamingWriter, InvalidRecordsPerChunkUsesDefault)
{
    using UnaryWriter = TestDAGWriter<UnaryDAGResponseWriter>;
    using StreamingWriter = TestDAGWriter<StreamingDAGResponseWriter<std::shared_ptr<MockStreamWriter>>>;
    const std::vector<Int64> chunk_sizes{0, -2, std::numeric_limits<Int64>::min()};
    const Block block = prepareBlock(DEFAULT_DAG_RECORDS_PER_CHUNK + 1);
    for (auto encode_type : {tipb::TypeDefault, tipb::TypeChunk, tipb::TypeCHBlock})
    {
        SCOPED_TRACE(encode_type);
        dag_context_ptr->encode_type = encode_type;
        tipb::SelectResponse expected;
        UnaryDAGResponseWriter reference(&expected, DEFAULT_DAG_RECORDS_PER_CHUNK, *dag_context_ptr);
        reference.write(block);
        reference.flush();
        ASSERT_EQ(expected.chunks_size(), encode_type == tipb::TypeCHBlock ? 1 : 2);

        for (Int64 records_per_chunk : chunk_sizes)
        {
            SCOPED_TRACE(records_per_chunk);
            tipb::SelectResponse response;
            UnaryWriter unary_writer(&response, records_per_chunk, *dag_context_ptr);
            ASSERT_TRUE(unary_writer.hasValidChunkSize());
            unary_writer.write(block);
            unary_writer.flush();

            std::vector<tipb::SelectResponse> write_report;
            auto mock_writer = std::make_shared<MockStreamWriter>(
                [&](tipb::SelectResponse & resp) { write_report.emplace_back(std::move(resp)); });
            StreamingWriter streaming_writer(mock_writer, records_per_chunk, 1, 16 * 1024 * 1024, *dag_context_ptr);
            ASSERT_TRUE(streaming_writer.hasValidChunkSize());
            streaming_writer.write(block);
            streaming_writer.flush();

            ASSERT_EQ(response.chunks_size(), expected.chunks_size());
            ASSERT_EQ(write_report.size(), 1);
            ASSERT_EQ(write_report.front().chunks_size(), expected.chunks_size());
            for (int i = 0; i < expected.chunks_size(); ++i)
            {
                EXPECT_EQ(response.chunks(i).rows_data(), expected.chunks(i).rows_data());
                EXPECT_EQ(write_report.front().chunks(i).rows_data(), expected.chunks(i).rows_data());
            }
        }
    }
}

TEST_F(TestStreamingWriter, AllowsNonzeroRecordsPerChunk)
{
    using MockDAGWriter = StreamingDAGResponseWriter<std::shared_ptr<MockStreamWriter>>;
    const std::vector<Int64> chunk_sizes{-1, 1, DEFAULT_DAG_RECORDS_PER_CHUNK};
    auto mock_writer = std::make_shared<MockStreamWriter>([](tipb::SelectResponse &) {});
    for (auto encode_type : {tipb::TypeDefault, tipb::TypeChunk, tipb::TypeCHBlock})
    {
        SCOPED_TRACE(encode_type);
        dag_context_ptr->encode_type = encode_type;
        for (Int64 records_per_chunk : chunk_sizes)
        {
            SCOPED_TRACE(records_per_chunk);
            tipb::SelectResponse response;
            EXPECT_NO_THROW(UnaryDAGResponseWriter(&response, records_per_chunk, *dag_context_ptr));
            EXPECT_NO_THROW(MockDAGWriter(mock_writer, records_per_chunk, 1, 1024, *dag_context_ptr));
        }
    }
}

TEST_F(TestStreamingWriter, testStreamingWriter)
try
{
    std::vector<tipb::EncodeType> encode_types{
        tipb::EncodeType::TypeDefault,
        tipb::EncodeType::TypeChunk,
        tipb::EncodeType::TypeCHBlock};
    for (const auto & encode_type : encode_types)
    {
        dag_context_ptr->encode_type = encode_type;

        const size_t block_rows = 64;
        const size_t block_num = 64;
        const size_t batch_send_min_limit = 108;
        const size_t max_buffered_bytes = 1024 * 1024 * 16;

        // 1. Build Blocks.
        std::vector<Block> blocks;
        for (size_t i = 0; i < block_num; ++i)
        {
            blocks.emplace_back(prepareBlock(block_rows));
            blocks.emplace_back(prepareBlock(0));
        }
        Block header = blocks.back();

        // 2. Build MockStreamWriter.
        std::vector<tipb::SelectResponse> write_report;
        auto checker = [&write_report](tipb::SelectResponse & response) {
            write_report.emplace_back(std::move(response));
        };
        auto mock_writer = std::make_shared<MockStreamWriter>(checker);

        // 3. Start to write.
        auto dag_writer = std::make_shared<StreamingDAGResponseWriter<std::shared_ptr<MockStreamWriter>>>(
            mock_writer,
            batch_send_min_limit,
            batch_send_min_limit,
            max_buffered_bytes,
            *dag_context_ptr);
        for (const auto & block : blocks)
            dag_writer->write(block);
        dag_writer->flush();

        // 4. Start to check write_report.
        size_t expect_rows = block_rows * block_num;
        size_t decoded_block_rows = 0;
        for (const auto & resp : write_report)
        {
            for (int i = 0; i < resp.chunks_size(); ++i)
            {
                const tipb::Chunk & chunk = resp.chunks(i);
                Block decoded_block;
                switch (encode_type)
                {
                case tipb::EncodeType::TypeDefault:
                    decoded_block = DefaultChunkCodec().decode(chunk.rows_data(), makeSchema());
                    break;
                case tipb::EncodeType::TypeChunk:
                    decoded_block = ArrowChunkCodec().decode(chunk.rows_data(), makeSchema());
                    break;
                case tipb::EncodeType::TypeCHBlock:
                    decoded_block = CHBlockChunkCodec::decode(chunk.rows_data(), header);
                    break;
                }
                decoded_block_rows += decoded_block.rows();
            }
        }
        ASSERT_EQ(decoded_block_rows, expect_rows);
    }
}
CATCH

TEST_F(TestStreamingWriter, testMaxBufferedBytes)
try
{
    std::vector<tipb::EncodeType> encode_types{
        tipb::EncodeType::TypeDefault,
        tipb::EncodeType::TypeChunk,
        tipb::EncodeType::TypeCHBlock};
    for (const auto & encode_type : encode_types)
    {
        dag_context_ptr->encode_type = encode_type;

        const size_t block_rows = 64;
        const size_t block_num = 64;
        const size_t batch_send_min_limit = block_rows * block_num / 2;
        const std::vector<UInt64> max_buffered_bytes_vec{1, 1024 * 1024 * 1024};
        const std::vector<UInt64> expected_response_num{block_num, 2};

        for (size_t i = 0; i < max_buffered_bytes_vec.size(); ++i)
        {
            // 1. Build Blocks.
            std::vector<Block> blocks;
            for (size_t i = 0; i < block_num; ++i)
            {
                blocks.emplace_back(prepareBlock(block_rows));
                blocks.emplace_back(prepareBlock(0));
            }
            Block header = blocks.back();

            // 2. Build MockStreamWriter.
            std::vector<tipb::SelectResponse> write_report;
            auto checker = [&write_report](tipb::SelectResponse & response) {
                write_report.emplace_back(std::move(response));
            };
            auto mock_writer = std::make_shared<MockStreamWriter>(checker);

            // 3. Write blocks
            auto dag_writer = std::make_shared<StreamingDAGResponseWriter<std::shared_ptr<MockStreamWriter>>>(
                mock_writer,
                batch_send_min_limit,
                batch_send_min_limit,
                max_buffered_bytes_vec[i],
                *dag_context_ptr);
            for (const auto & block : blocks)
                dag_writer->write(block);
            dag_writer->flush();

            ASSERT_EQ(write_report.size(), expected_response_num[i]);
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
