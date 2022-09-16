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

<<<<<<< HEAD
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <gtest/gtest.h>

#include "RowCodecTestUtils.h"
=======
#include <Common/hex.h>
#include <Core/Field.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/PartitionStreams.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/tests/RowCodecTestUtils.h>
#include <Storages/Transaction/tests/region_helper.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>
#include <common/logger_useful.h>
>>>>>>> 8e411ae86b (Fix decode error when "NULL" value in the column with "primary key" flag (#5879))

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
<<<<<<< HEAD
            if (!table_info.columns[i].hasPriKeyFlag())
                value_fields.emplace_back(fields[i]);
=======
            if (table_info.is_common_handle || table_info.pk_is_handle)
            {
                if (table_info.columns[i].hasPriKeyFlag())
                    key_encode_fields.emplace_back(fields[i]);
                else
                    value_encode_fields.emplace_back(fields[i]);
            }
>>>>>>> 8e411ae86b (Fix decode error when "NULL" value in the column with "primary key" flag (#5879))
            else
                pk_fields.emplace_back(fields[i]);
        }

        // create PK
        WriteBufferFromOwnString pk_buf;
        if (table_info.is_common_handle)
        {
            const auto & primary_index_info = table_info.getPrimaryIndexInfo();
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
<<<<<<< HEAD
                    ASSERT_EQ((*column_element.column)[row], fields_map_.at(column_element.column_id));
=======
                    if (fields_map.count(column_element.column_id) > 0)
                        ASSERT_FIELD_EQ((*column_element.column)[row], fields_map.at(column_element.column_id)) << gen_error_log();
                    else
                        LOG_INFO(logger, "ignore value check for new added column, id={}, name={}", column_element.column_id, column_element.name);
>>>>>>> 8e411ae86b (Fix decode error when "NULL" value in the column with "primary key" flag (#5879))
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

<<<<<<< HEAD
TEST_F(RegionBlockReaderTestFixture, PKIsNotHandle)
=======
String bytesFromHexString(std::string_view hex_str)
{
    assert(hex_str.size() % 2 == 0);
    String bytes(hex_str.size() / 2, '\x00');
    for (size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = unhex2(hex_str.data() + i * 2);
    }
    return bytes;
}

TEST_F(RegionBlockReaderTest, PKIsNotHandle)
>>>>>>> 8e411ae86b (Fix decode error when "NULL" value in the column with "primary key" flag (#5879))
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


TEST_F(RegionBlockReaderTest, MissingPrimaryKeyColumnRowV2)
try
{
    // Mock a table
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    column4 varchar(20) DEFAULT NULL,
    //    primary key (`column3`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    auto [table_info, fields] = getTableInfoAndFields(/*pk_col_ids*/ {3}, false, ColumnIDValue<String>(3, "hello"), ColumnIDValueNull<String>(4));
    ASSERT_EQ(table_info.is_common_handle, false);
    ASSERT_EQ(table_info.pk_is_handle, false);
    ASSERT_TRUE(table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_FALSE(table_info.getColumnInfo(4).hasPriKeyFlag());

    // FIXME: actually TiDB won't encode the "NULL" for column4 into value
    // but now the `RowEncoderV2` does not support this, we use `RegionBlockReaderTest::ReadFromRegion`
    // to test that.
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);

    // Mock re-create the primary key index with "column4" that contains `NULL` value
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    column4 varchar(20) NOT NULL,
    //    primary key (`column3`, `column4`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    TableInfo new_table_info;
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(/*pk_col_ids*/ {3, 4}, false, ColumnIDValueNull<String>(3), ColumnIDValueNull<String>(4));
    ASSERT_EQ(new_table_info.is_common_handle, false);
    ASSERT_EQ(new_table_info.pk_is_handle, false);
    ASSERT_TRUE(new_table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_TRUE(new_table_info.getColumnInfo(4).hasPriKeyFlag());

    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    // FIXME: actually we need to decode the block with force_decode=true, see the
    // comments before `encodeColumns`
    EXPECT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
    // // force_decode=false can not decode because there are
    // // missing value for column with primary key flag.
    // EXPECT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    // // force_decode=true, decode ok.
    // EXPECT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}
CATCH

TEST_F(RegionBlockReaderTest, MissingPrimaryKeyColumnRowV1)
try
{
    // Mock a table
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    column4 varchar(20) DEFAULT NULL,
    //    primary key (`column3`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    auto [table_info, fields] = getTableInfoAndFields(/*pk_col_ids*/ {3}, false, ColumnIDValue<String>(3, "hello"), ColumnIDValueNull<String>(4));
    ASSERT_EQ(table_info.is_common_handle, false);
    ASSERT_EQ(table_info.pk_is_handle, false);
    ASSERT_TRUE(table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_FALSE(table_info.getColumnInfo(4).hasPriKeyFlag());

    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);

    // Mock re-create the primary key index with "column4" that contains `NULL` value
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    column4 varchar(20) NOT NULL,
    //    primary key (`column3`, `column4`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    TableInfo new_table_info;
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(/*pk_col_ids*/ {3, 4}, false, ColumnIDValueNull<String>(3), ColumnIDValueNull<String>(4));
    ASSERT_EQ(new_table_info.is_common_handle, false);
    ASSERT_EQ(new_table_info.pk_is_handle, false);
    ASSERT_TRUE(new_table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_TRUE(new_table_info.getColumnInfo(4).hasPriKeyFlag());

    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    EXPECT_TRUE(decodeAndCheckColumns(new_decoding_schema, false));
}
CATCH

TEST_F(RegionBlockReaderTest, NewMissingPrimaryKeyColumnRowV2)
try
{
    // Mock a table
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    primary key (`column3`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    auto [table_info, fields] = getTableInfoAndFields(/*pk_col_ids*/ {3}, false, ColumnIDValue<String>(3, "hello"));
    ASSERT_EQ(table_info.is_common_handle, false);
    ASSERT_EQ(table_info.pk_is_handle, false);
    ASSERT_TRUE(table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_ANY_THROW(table_info.getColumnInfo(4)); // not exist

    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);

    // Mock re-create the primary key index with new-added "column4"
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    column4 varchar(20) NOT NULL,
    //    primary key (`column3`, `column4`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    TableInfo new_table_info;
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(/*pk_col_ids*/ {3, 4}, false, ColumnIDValueNull<String>(3), ColumnIDValueNull<String>(4));
    ASSERT_EQ(new_table_info.is_common_handle, false);
    ASSERT_EQ(new_table_info.pk_is_handle, false);
    ASSERT_TRUE(new_table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_TRUE(new_table_info.getColumnInfo(4).hasPriKeyFlag());

    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    // force_decode=false can not decode because there are
    // missing value for column with primary key flag.
    EXPECT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    // force_decode=true, decode ok.
    EXPECT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}
CATCH

TEST_F(RegionBlockReaderTest, NewMissingPrimaryKeyColumnRowV1)
try
{
    // Mock a table
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    primary key (`column3`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    auto [table_info, fields] = getTableInfoAndFields(/*pk_col_ids*/ {3}, false, ColumnIDValue<String>(3, "hello"));
    ASSERT_EQ(table_info.is_common_handle, false);
    ASSERT_EQ(table_info.pk_is_handle, false);
    ASSERT_TRUE(table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_ANY_THROW(table_info.getColumnInfo(4)); // not exist

    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);

    // Mock re-create the primary key index with new-added "column4"
    // `t_case` {
    //    column3 varchar(32) NOT NULL,
    //    column4 varchar(20) NOT NULL,
    //    primary key (`column3`, `column4`) /*T![clustered_index] NONCLUSTERED */
    //    -- _tidb_rowid bigint, // hidden handle
    // }
    TableInfo new_table_info;
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(/*pk_col_ids*/ {3, 4}, false, ColumnIDValueNull<String>(3), ColumnIDValueNull<String>(4));
    ASSERT_EQ(new_table_info.is_common_handle, false);
    ASSERT_EQ(new_table_info.pk_is_handle, false);
    ASSERT_TRUE(new_table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_TRUE(new_table_info.getColumnInfo(4).hasPriKeyFlag());

    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    // force_decode=false can not decode because there are
    // missing value for column with primary key flag.
    EXPECT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    // force_decode=true, decode ok.
    EXPECT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}
CATCH

TEST_F(RegionBlockReaderTest, ReadFromRegion)
try
{
    TableInfo table_info(R"({"cols":[
        {"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"case_no","O":"case_no"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":4099,"Flen":32,"Tp":15}},
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"p","O":"p"},"offset":1,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":0,"Flen":12,"Tp":15}},
        {"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"source","O":"source"},"offset":2,"origin_default":"","state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":4099,"Flen":20,"Tp":15}}
    ],"comment":"","id":77,"index_info":[],"is_common_handle":false,"name":{"L":"t_case","O":"t_case"},"partition":null,"pk_is_handle":false,"schema_version":62,"state":5,"tiflash_replica":{"Count":1},"update_timestamp":435984541435559947})");

    RegionID region_id = 4;
    String region_start_key(bytesFromHexString("7480000000000000FF445F720000000000FA"));
    String region_end_key(bytesFromHexString("7480000000000000FF4500000000000000F8"));
    auto region = makeRegion(region_id, region_start_key, region_end_key);
    // the hex kv dump from SSTFile
    std::vector<std::tuple<std::string_view, std::string_view>> kvs = {
        {"7480000000000000FF4D5F728000000000FF0000010000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000010000000000FAF9F3126548ABFFFC", "508180D0BAABB3BB8606760A80000100000001010031"},
        {"7480000000000000FF4D5F728000000000FF0000020000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000020000000000FAF9F3126548ABFFFC", "508180D0BAABB3BB8606760A80000100000001010032"},
        {"7480000000000000FF4D5F728000000000FF0000030000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000030000000000FAF9F3126548ABFFFC", "508180D0BAABB3BB8606760A80000100000001010033"},
        {"7480000000000000FF4D5F728000000000FF0000040000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000040000000000FAF9F3126548ABFFFC", "508180D0BAABB3BB8606760A80000100000001010034"},
    };
    for (const auto & [k, v] : kvs)
    {
        region->insert(ColumnFamilyType::Write, TiKVKey(bytesFromHexString(k)), TiKVValue(bytesFromHexString(v)));
    }

    auto data_list_read = ReadRegionCommitCache(region, true);
    ASSERT_TRUE(data_list_read.has_value());

    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    {
        // force_decode=false can not decode because there are
        // missing value for column with primary key flag.
        auto reader = RegionBlockReader(decoding_schema);
        Block res_block = createBlockSortByColumnID(decoding_schema);
        EXPECT_FALSE(reader.read(res_block, *data_list_read, false));
    }
    {
        // force_decode=true can decode the block
        auto reader = RegionBlockReader(decoding_schema);
        Block res_block = createBlockSortByColumnID(decoding_schema);
        EXPECT_TRUE(reader.read(res_block, *data_list_read, true));
        res_block.checkNumberOfRows();
        EXPECT_EQ(res_block.rows(), 4);
        ASSERT_COLUMN_EQ(res_block.getByName("case_no"), createColumn<String>({"1", "2", "3", "4"}));
    }
}
CATCH


} // namespace DB::tests
