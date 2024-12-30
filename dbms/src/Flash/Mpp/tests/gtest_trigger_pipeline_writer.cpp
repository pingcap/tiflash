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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Coprocessor/WaitResult.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

#include <random>

namespace DB
{
namespace tests
{
class MockPipelineTriggerWriter : public DAGResponseWriter
{
public:
    MockPipelineTriggerWriter(Int64 records_per_chunk, DAGContext & dag_context)
        : DAGResponseWriter(records_per_chunk, dag_context)
        , rng(dev())
    {}
    bool doWrite(const Block &) override
    {
        std::uniform_int_distribution<Int32> dist;
        auto next = dist(rng);
        return next % 3 == 1;
    }
    bool doFlush() override
    {
        std::uniform_int_distribution<Int32> dist;
        auto next = dist(rng);
        return next % 3 == 0;
    }
    void notifyNextPipelineWriter() override { writer_notify_count--; }
    WaitResult waitForWritable() const override
    {
        std::uniform_int_distribution<Int32> dist;
        auto next = dist(rng);
        if (next % 3 == 1)
        {
            writer_notify_count++;
            return WaitResult::WaitForNotify;
        }
        return WaitResult::Ready;
    }
    Int64 getPipelineNotifyCount() const { return writer_notify_count; }

private:
    mutable Int64 writer_notify_count = 0;
    std::random_device dev;
    mutable std::mt19937 rng;
};

class TestTriggerPipelineWriter : public testing::Test
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
    TestTriggerPipelineWriter() = default;

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

    std::unique_ptr<DAGContext> dag_context_ptr;
};

TEST_F(TestTriggerPipelineWriter, testPipelineWriter)
try
{
    const size_t block_rows = 1024;
    // 1. Build Block.
    auto block = prepareRandomBlock(block_rows);

    for (int test_index = 0; test_index < 100; test_index++)
    {
        // 2. Build MockWriter.
        auto mock_writer = std::make_shared<MockPipelineTriggerWriter>(-1, *dag_context_ptr);

        // 3. write something
        for (int i = 0; i < 100; i++)
        {
            mock_writer->waitForWritable();
            mock_writer->write(block);
        }

        // 4. flush
        mock_writer->waitForWritable();
        mock_writer->flush();

        // 5. check results, note redudent notify is allowed
        ASSERT_EQ(true, mock_writer->getPipelineNotifyCount() <= 0);
    }
}
CATCH

} // namespace tests
} // namespace DB
