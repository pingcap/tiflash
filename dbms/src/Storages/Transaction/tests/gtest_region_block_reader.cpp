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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <gtest/gtest.h>

#include <TIDB/Codec/tests/RowCodecTestUtils.h>

using TableInfo = TiDB::TableInfo;

namespace DB::tests
{
using ColumnIDs = std::vector<ColumnID>;
class RegionBlockReaderTestFixture : public ::testing::Test
{
protected:
    Int64 handle_value_ = 100;
    UInt8 del_mark_value_ = 0;
    UInt64 version_value_ = 100;
    size_t rows_ = 3;

    RegionDataReadInfoList data_list_read_;
    std::unordered_map<ColumnID, Field> fields_map_;

    enum RowEncodeVersion
    {
        RowV1,
        RowV2
    };

protected:
    void SetUp() override
    {
        data_list_read_.clear();
        fields_map_.clear();
    }

    void TearDown() override {}

    void encodeColumns(TableInfo & table_info, std::vector<Field> & fields, RowEncodeVersion row_version)
    {
        // for later check
        for (size_t i = 0; i < table_info.columns.size(); i++)
            fields_map_.emplace(table_info.columns[i].id, fields[i]);

        std::vector<Field> value_fields;
        std::vector<Field> pk_fields;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            if (!table_info.columns[i].hasPriKeyFlag())
                value_fields.emplace_back(fields[i]);
            else
                pk_fields.emplace_back(fields[i]);
        }

        // create PK
        WriteBufferFromOwnString pk_buf;
        if (table_info.is_common_handle)
        {
            auto & primary_index_info = table_info.getPrimaryIndexInfo();
            for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
            {
                size_t pk_offset = primary_index_info.idx_cols[i].offset;
                EncodeDatum(pk_fields[i], table_info.columns[pk_offset].getCodecFlag(), pk_buf);
            }
        }
        else
        {
            DB::EncodeInt64(handle_value_, pk_buf);
        }
        RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
        // create value
        WriteBufferFromOwnString value_buf;
        if (row_version == RowEncodeVersion::RowV1)
        {
            encodeRowV1(table_info, value_fields, value_buf);
        }
        else if (row_version == RowEncodeVersion::RowV2)
        {
            encodeRowV2(table_info, value_fields, value_buf);
        }
        else
        {
            throw Exception("Unknown row format " + std::to_string(row_version), ErrorCodes::LOGICAL_ERROR);
        }
        auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
        for (size_t i = 0; i < rows_; i++)
            data_list_read_.emplace_back(pk, del_mark_value_, version_value_, row_value);
    }

    void checkBlock(DecodingStorageSchemaSnapshotConstPtr decoding_schema, const Block & block) const
    {
        ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
        for (size_t row = 0; row < rows_; row++)
        {
            for (size_t pos = 0; pos < block.columns(); pos++)
            {
                auto & column_element = block.getByPosition(pos);
                if (row == 0)
                {
                    ASSERT_EQ(column_element.column->size(), rows_);
                }
                if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                {
                    if (decoding_schema->is_common_handle)
                    {
                        ASSERT_EQ((*column_element.column)[row], Field(*std::get<0>(data_list_read_[row])));
                    }
                    else
                    {
                        ASSERT_EQ((*column_element.column)[row], Field(handle_value_));
                    }
                }
                else if (column_element.name == VERSION_COLUMN_NAME)
                {
                    ASSERT_EQ((*column_element.column)[row], Field(version_value_));
                }
                else if (column_element.name == TAG_COLUMN_NAME)
                {
                    ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value_)));
                }
                else
                {
                    ASSERT_EQ((*column_element.column)[row], fields_map_.at(column_element.column_id));
                }
            }
        }
    }

    bool decodeAndCheckColumns(DecodingStorageSchemaSnapshotConstPtr decoding_schema, bool force_decode) const
    {
        RegionBlockReader reader{decoding_schema};
        Block block = createBlockSortByColumnID(decoding_schema);
        if (!reader.read(block, data_list_read_, force_decode))
            return false;

        checkBlock(decoding_schema, block);
        return true;
    }

    std::pair<TableInfo, std::vector<Field>> getNormalTableInfoFields(const ColumnIDs & handle_ids, bool is_common_handle) const
    {
        return getTableInfoAndFields(
            handle_ids,
            is_common_handle,
            ColumnIDValue(2, handle_value_),
            ColumnIDValue(3, std::numeric_limits<UInt64>::max()),
            ColumnIDValue(4, std::numeric_limits<Float32>::min()),
            ColumnIDValue(9, String("aaa")),
            ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
            ColumnIDValueNull<UInt64>(11));
    }

    TableInfo getTableInfoWithMoreColumns(const ColumnIDs & handle_ids, bool is_common_handle)
    {
        TableInfo table_info;
        std::tie(table_info, std::ignore) = getTableInfoAndFields(
            handle_ids,
            is_common_handle,
            ColumnIDValue(1, String("")),
            ColumnIDValue(2, handle_value_),
            ColumnIDValue(3, std::numeric_limits<UInt64>::max()),
            ColumnIDValue(4, std::numeric_limits<Float32>::min()),
            ColumnIDValue(8, String("")),
            ColumnIDValue(9, String("aaa")),
            ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
            ColumnIDValueNull<UInt64>(11),
            ColumnIDValue(13, String("")));

        // add default value for missing column
        std::vector<ColumnID> missing_column_ids{1, 8, 13};
        String missing_column_default_value = String("default");
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            if (std::find(missing_column_ids.begin(), missing_column_ids.end(), table_info.columns[i].id) != missing_column_ids.end())
            {
                table_info.columns[i].origin_default_value = missing_column_default_value;
                fields_map_.emplace(table_info.columns[i].id, Field(missing_column_default_value));
            }
        }
        return table_info;
    }

    TableInfo getTableInfoWithLessColumns(const ColumnIDs & handle_ids, bool is_common_handle) const
    {
        TableInfo table_info;
        std::tie(table_info, std::ignore) = getTableInfoAndFields(
            handle_ids,
            is_common_handle,
            ColumnIDValue(2, handle_value_),
            ColumnIDValue(4, std::numeric_limits<Float32>::min()),
            ColumnIDValue(9, String("aaa")),
            ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)));
        return table_info;
    }

    TableInfo getTableInfoWithMoreNarrowIntType(const ColumnIDs & handle_ids, bool is_common_handle) const
    {
        TableInfo table_info;
        std::tie(table_info, std::ignore) = getTableInfoAndFields(
            handle_ids,
            is_common_handle,
            ColumnIDValue(2, handle_value_),
            ColumnIDValue(3, std::numeric_limits<UInt8>::max()),
            ColumnIDValue(4, std::numeric_limits<Float32>::min()),
            ColumnIDValue(9, String("aaa")),
            ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
            ColumnIDValueNull<UInt64>(11));
        return table_info;
    }

    TableInfo getTableInfoFieldsForInvalidNULLTest(const ColumnIDs & handle_ids, bool is_common_handle) const
    {
        TableInfo table_info;
        std::tie(table_info, std::ignore) = getTableInfoAndFields(
            handle_ids,
            is_common_handle,
            ColumnIDValue(2, handle_value_),
            ColumnIDValue(3, std::numeric_limits<UInt64>::max()),
            ColumnIDValue(4, std::numeric_limits<Float32>::min()),
            ColumnIDValue(9, String("aaa")),
            ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
            ColumnIDValue(11, std::numeric_limits<UInt64>::min()));
        return table_info;
    }
};

TEST_F(RegionBlockReaderTestFixture, PKIsNotHandle)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, PKIsHandle)
{
    auto [table_info, fields] = getNormalTableInfoFields({2}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, CommonHandle)
{
    auto [table_info, fields] = getNormalTableInfoFields({2, 3, 4}, true);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, MissingColumnRowV2)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto new_table_info = getTableInfoWithMoreColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, false));
}

TEST_F(RegionBlockReaderTestFixture, MissingColumnRowV1)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);
    auto new_table_info = getTableInfoWithMoreColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, false));
}

TEST_F(RegionBlockReaderTestFixture, ExtraColumnRowV2)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto new_table_info = getTableInfoWithLessColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, ExtraColumnRowV1)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);
    auto new_table_info = getTableInfoWithLessColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, OverflowColumnRowV2)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto new_table_info = getTableInfoWithMoreNarrowIntType({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_ANY_THROW(decodeAndCheckColumns(new_decoding_schema, true));

    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, OverflowColumnRowV1)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);
    auto new_table_info = getTableInfoWithMoreNarrowIntType({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_ANY_THROW(decodeAndCheckColumns(new_decoding_schema, true));

    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, InvalidNULLRowV2)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto new_table_info = getTableInfoFieldsForInvalidNULLTest({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_ANY_THROW(decodeAndCheckColumns(new_decoding_schema, true));
}

TEST_F(RegionBlockReaderTestFixture, InvalidNULLRowV1)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);
    auto new_table_info = getTableInfoFieldsForInvalidNULLTest({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_ANY_THROW(decodeAndCheckColumns(new_decoding_schema, true));
}

} // namespace DB::tests
