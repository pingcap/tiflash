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

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <cstring>
#include <limits>
#include <vector>

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

    DataTypeString a{DataTypeString::SerdesFormat::SizePrefix};
    DataTypeString b{DataTypeString::SerdesFormat::SeparateSizeAndChars};
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

TEST_F(DataTypeStringTest, RejectCorruptStringV2)
try
{
    DataTypeString type(DataTypeString::SerdesFormat::SeparateSizeAndChars);

    auto decode = [&](IColumn & column, const std::vector<UInt64> & sizes, const String & chars, size_t limit) {
        String size_buf(sizeof(UInt64) * sizes.size(), '\0');
        if (!sizes.empty())
            memcpy(size_buf.data(), sizes.data(), size_buf.size());
        ReadBufferFromString sizes_stream(size_buf);
        ReadBufferFromString chars_stream(chars);
        IDataType::SubstreamPath path;
        type.deserializeBinaryBulkWithMultipleStreams(
            column,
            [&](const IDataType::SubstreamPath & substream_path) -> ReadBuffer * {
                if (IDataType::isStringSizes(substream_path))
                    return &sizes_stream;
                return &chars_stream;
            },
            limit,
            0,
            true,
            path);
    };

    const auto assert_consistent = [](const IColumn & column) {
        const auto & column_string = typeid_cast<const ColumnString &>(column);
        ASSERT_FALSE(column_string.getOffsets().empty());
        ASSERT_EQ(column_string.getOffsets().back(), column_string.getChars().size());
    };

    {
        auto column = type.createColumn();
        decode(*column, {1, 2}, String("\0a\0", 3), 2);
        decode(*column, {3}, String("bc\0", 3), 1);
        ASSERT_EQ(column->size(), 3);
        ASSERT_EQ(column->getDataAt(0).toStringView(), "");
        ASSERT_EQ(column->getDataAt(1).toStringView(), "a");
        ASSERT_EQ(column->getDataAt(2).toStringView(), "bc");
        assert_consistent(*column);
    }

    auto expect_rejected
        = [&](const std::vector<UInt64> & sizes, const String & chars, size_t limit, int expected_code) {
              auto column = type.createColumn();
              decode(*column, {1}, String(1, '\0'), 1);
              try
              {
                  decode(*column, sizes, chars, limit);
                  ADD_FAILURE() << "corrupt StringV2 data was accepted";
              }
              catch (const Exception & e)
              {
                  EXPECT_EQ(e.code(), expected_code) << e.displayText();
              }
              ASSERT_EQ(column->size(), 1);
              ASSERT_EQ(column->getDataAt(0).toStringView(), "");
              assert_consistent(*column);
          };

    // size 0 makes ColumnString::sizeAt() - 1 wrap to SIZE_MAX.
    expect_rejected({0}, {}, 1, ErrorCodes::LOGICAL_ERROR);
    // 2^63 + (2^63 + 4) wraps to 4, so the char buffer would be 4 bytes while the first offset is 2^63.
    expect_rejected({1ULL << 63, (1ULL << 63) + 4}, String("abcd", 4), 2, ErrorCodes::LOGICAL_ERROR);
    // Appended to a column whose offset is already 1, this size overflows the running offset.
    expect_rejected({std::numeric_limits<UInt64>::max()}, {}, 1, ErrorCodes::LOGICAL_ERROR);
    // One byte is already stored. 2^63 - 1 fits in size_t but the padded power-of-two allocation does not.
    expect_rejected({(1ULL << 63) - 1}, {}, 1, ErrorCodes::LOGICAL_ERROR);
    // One byte is already stored. UINT64_MAX - 1 passes a size_t sum check and wraps the real allocation to 128 bytes.
    expect_rejected({std::numeric_limits<UInt64>::max() - 1}, {}, 1, ErrorCodes::LOGICAL_ERROR);
    // `limit` is allocated up front. The largest multiple of sizeof(UInt64) still overflows once padding is added.
    expect_rejected(
        {1},
        String(1, '\0'),
        std::numeric_limits<size_t>::max() / sizeof(UInt64),
        ErrorCodes::LOGICAL_ERROR);
    // The size is valid, but the character stream is shorter than that size.
    expect_rejected({4}, String(1, 'a'), 1, ErrorCodes::CANNOT_READ_ALL_DATA);

    // A short final batch is valid: `limit` is an upper bound and the stream may end first.
    {
        auto column = type.createColumn();
        decode(*column, {1}, String(1, '\0'), 1);
        decode(*column, {2}, String("a\0", 2), 4);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ(column->getDataAt(0).toStringView(), "");
        ASSERT_EQ(column->getDataAt(1).toStringView(), "a");
        assert_consistent(*column);
    }

    // A trailing partial UInt64 is not a short batch.
    {
        auto column = type.createColumn();
        decode(*column, {1}, String(1, '\0'), 1);
        String size_buf(sizeof(UInt64) + 1, '\0');
        const UInt64 sz = 1;
        memcpy(size_buf.data(), &sz, sizeof(sz));
        ReadBufferFromString sizes_stream(size_buf);
        ReadBufferFromString chars_stream(String(1, '\0'));
        IDataType::SubstreamPath path;
        try
        {
            type.deserializeBinaryBulkWithMultipleStreams(
                *column,
                [&](const IDataType::SubstreamPath & substream_path) -> ReadBuffer * {
                    if (IDataType::isStringSizes(substream_path))
                        return &sizes_stream;
                    return &chars_stream;
                },
                2,
                0,
                true,
                path);
            ADD_FAILURE() << "a torn StringV2 size was accepted";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        }
        ASSERT_EQ(column->size(), 1);
        ASSERT_EQ(column->getDataAt(0).toStringView(), "");
        assert_consistent(*column);
    }
}
CATCH

} // namespace DB::tests
