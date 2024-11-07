// Copyright 2024 PingCAP, Inc.
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

#include <DataTypes/DataTypeString.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
namespace DB::tests
{
class DataTypeStringTest : public ::testing::Test
{
public:
    void SetUp() override {}

    void TearDown() override {}

protected:
    static String getStreamName(const String & column_name, const IDataType::SubstreamPath & substream_path)
    {
        return IDataType::getFileNameForStream(column_name, substream_path);
    }

    void initWriteStream()
    {
        auto create_write_stream = [&](const String & column_name, const IDataType::SubstreamPath & substream_path) {
            const auto stream_name = getStreamName(column_name, substream_path);
            write_streams.emplace(stream_name, std::make_unique<WriteBufferFromOwnString>(10 * 1024 * 1024));
        };
        auto create_write_stream1 = [&](const IDataType::SubstreamPath & substream_path) {
            create_write_stream("1", substream_path);
        };
        auto create_write_stream2 = [&](const IDataType::SubstreamPath & substream_path) {
            create_write_stream("2", substream_path);
        };
        str_v0.enumerateStreams(create_write_stream1, {});
        str_v1.enumerateStreams(create_write_stream2, {});
    }

    void initReadStream()
    {
        auto create_read_stream = [&](const String & column_name, const IDataType::SubstreamPath & substream_path) {
            const auto stream_name = getStreamName(column_name, substream_path);
            auto s = write_streams.at(stream_name)->stringRef().toStringView();
            read_streams.emplace(stream_name, std::make_unique<ReadBufferFromString>(s));
        };
        auto create_read_stream1 = [&](const IDataType::SubstreamPath & substream_path) {
            create_read_stream("1", substream_path);
        };
        auto create_read_stream2 = [&](const IDataType::SubstreamPath & substream_path) {
            create_read_stream("2", substream_path);
        };
        str_v0.enumerateStreams(create_read_stream1, {});
        str_v1.enumerateStreams(create_read_stream2, {});
    }

    void serialize(const IColumn & col, size_t offset, size_t limit)
    {
        auto get_write_stream = [&](const String & column_name, const IDataType::SubstreamPath & substream_path) {
            const auto stream_name = getStreamName(column_name, substream_path);
            return write_streams.at(stream_name).get();
        };
        auto get_write_stream1 = [&](const IDataType::SubstreamPath & substream_path) {
            return get_write_stream("1", substream_path);
        };
        auto get_write_stream2 = [&](const IDataType::SubstreamPath & substream_path) {
            return get_write_stream("2", substream_path);
        };
        str_v0.serializeBinaryBulkWithMultipleStreams(col, get_write_stream1, offset, limit, true, {});
        str_v1.serializeBinaryBulkWithMultipleStreams(col, get_write_stream2, offset, limit, true, {});
    }

    void deserialize(IColumn & col1, IColumn & col2, size_t limit)
    {
        auto get_read_stream = [&](const String & column_name, const IDataType::SubstreamPath & substream_path) {
            const auto stream_name = getStreamName(column_name, substream_path);
            return read_streams.at(stream_name).get();
        };
        auto get_read_stream1 = [&](const IDataType::SubstreamPath & substream_path) {
            return get_read_stream("1", substream_path);
        };
        auto get_read_stream2 = [&](const IDataType::SubstreamPath & substream_path) {
            return get_read_stream("2", substream_path);
        };
        str_v0.deserializeBinaryBulkWithMultipleStreams(col1, get_read_stream1, limit, 8, true, {});
        str_v1.deserializeBinaryBulkWithMultipleStreams(col2, get_read_stream2, limit, 8, true, {});
    }

    DataTypeString a{0};
    DataTypeString b{1};
    IDataType & str_v0 = a;
    IDataType & str_v1 = b;

    std::unordered_map<String, std::unique_ptr<WriteBufferFromOwnString>> write_streams;
    std::unordered_map<String, std::unique_ptr<ReadBuffer>> read_streams;
};

TEST_F(DataTypeStringTest, BasicSerDe)
try
{
    auto str_col = DB::tests::createColumn<String>(DM::tests::createNumberStrings(0, 65536), "", 0).column;
    initWriteStream();
    ASSERT_EQ(write_streams.size(), 3);
    serialize(*str_col, 0, str_col->size());
    initReadStream();
    ASSERT_EQ(read_streams.size(), 3);
    auto col1 = str_v0.createColumn();
    auto col2 = str_v1.createColumn();
    deserialize(*col1, *col2, str_col->size());

    ASSERT_EQ(col1->size(), str_col->size());
    ASSERT_EQ(col2->size(), str_col->size());
    for (size_t i = 0; i < col2->size(); ++i)
    {
        ASSERT_EQ(col1->getDataAt(i).toStringView(), str_col->getDataAt(i).toStringView());
        ASSERT_EQ(col2->getDataAt(i).toStringView(), str_col->getDataAt(i).toStringView());
    }
}
CATCH

TEST_F(DataTypeStringTest, Concat)
try
{
    auto str_col = DB::tests::createColumn<String>(DM::tests::createNumberStrings(0, 65536), "", 0).column;
    initWriteStream();
    ASSERT_EQ(write_streams.size(), 3);
    serialize(*str_col, 0, 10000);
    serialize(*str_col, 10000, 20000);
    serialize(*str_col, 30000, 30000);
    serialize(*str_col, 60000, 40000);

    initReadStream();
    ASSERT_EQ(read_streams.size(), 3);
    auto col1 = str_v0.createColumn();
    auto col2 = str_v1.createColumn();
    deserialize(*col1, *col2, 20000);
    ASSERT_EQ(col1->size(), 20000);
    ASSERT_EQ(col2->size(), 20000);
    deserialize(*col1, *col2, 30000);
    ASSERT_EQ(col1->size(), 50000);
    ASSERT_EQ(col2->size(), 50000);
    deserialize(*col1, *col2, 10000);
    ASSERT_EQ(col1->size(), 60000);
    ASSERT_EQ(col2->size(), 60000);
    deserialize(*col1, *col2, 8000);

    ASSERT_EQ(col1->size(), str_col->size());
    ASSERT_EQ(col2->size(), str_col->size());
    for (size_t i = 0; i < col2->size(); ++i)
    {
        ASSERT_EQ(col1->getDataAt(i).toStringView(), str_col->getDataAt(i).toStringView());
        ASSERT_EQ(col2->getDataAt(i).toStringView(), str_col->getDataAt(i).toStringView());
    }
}
CATCH

} // namespace DB::tests
