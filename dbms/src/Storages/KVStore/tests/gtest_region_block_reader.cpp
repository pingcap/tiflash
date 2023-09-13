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

#include <Common/hex.h>
#include <Core/Field.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Decode/DecodingStorageSchemaSnapshot.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/Decode/RegionBlockReader.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/tests/RowCodecTestUtils.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Schema/TiDB.h>
#include <common/defines.h>
#include <common/logger_useful.h>

using TableInfo = TiDB::TableInfo;

namespace DB::tests
{
using ColumnIDs = std::vector<ColumnID>;
class RegionBlockReaderTest : public ::testing::Test
{
public:
    RegionBlockReaderTest()
        : logger(Logger::get())
    {}

protected:
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;
    size_t rows = 3;

    RegionDataReadInfoList data_list_read;
    std::unordered_map<ColumnID, Field> fields_map;

    LoggerPtr logger;

    enum RowEncodeVersion
    {
        RowV1,
        RowV2
    };

protected:
    void SetUp() override
    {
        data_list_read.clear();
        fields_map.clear();
    }

    void TearDown() override {}

    void encodeColumns(const TableInfo & table_info, const std::vector<Field> & fields, RowEncodeVersion row_version)
    {
        // for later check
        std::unordered_map<String, size_t> column_name_columns_index_map;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            fields_map.emplace(table_info.columns[i].id, fields[i]);
            column_name_columns_index_map.emplace(table_info.columns[i].name, i);
        }

        std::vector<Field> value_encode_fields;
        std::vector<Field> key_encode_fields;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            if (table_info.is_common_handle || table_info.pk_is_handle)
            {
                if (table_info.columns[i].hasPriKeyFlag())
                    key_encode_fields.emplace_back(fields[i]);
                else
                    value_encode_fields.emplace_back(fields[i]);
            }
            else
            {
                value_encode_fields.emplace_back(fields[i]);
            }
        }

        // create the RawTiDBPK section of encoded key
        WriteBufferFromOwnString pk_buf;
        if (table_info.is_common_handle)
        {
            const auto & primary_index_info = table_info.getPrimaryIndexInfo();
            for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
            {
                size_t pk_offset = primary_index_info.idx_cols[i].offset;
                EncodeDatum(key_encode_fields[i], table_info.columns[pk_offset].getCodecFlag(), pk_buf);
            }
        }
        else
        {
            DB::EncodeInt64(handle_value, pk_buf);
        }
        RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};

        // create encoded value
        WriteBufferFromOwnString value_buf;
        if (row_version == RowEncodeVersion::RowV1)
        {
            encodeRowV1(table_info, value_encode_fields, value_buf);
        }
        else if (row_version == RowEncodeVersion::RowV2)
        {
            encodeRowV2(table_info, value_encode_fields, value_buf);
        }
        else
        {
            throw Exception("Unknown row format " + std::to_string(row_version), ErrorCodes::LOGICAL_ERROR);
        }
        auto row_value = std::make_shared<const TiKVValue>(value_buf.releaseStr());
        for (size_t i = 0; i < rows; i++)
            data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);
    }

    void checkBlock(DecodingStorageSchemaSnapshotConstPtr decoding_schema, const Block & block) const
    {
        ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
        for (size_t row = 0; row < rows; row++)
        {
            for (size_t pos = 0; pos < block.columns(); pos++)
            {
                const auto & column_element = block.getByPosition(pos);
                auto gen_error_log = [&]() {
                    return fmt::format(
                        "  when checking column\n    id={}, name={}, nrow={}\n  decoded block is:\n{}\n",
                        column_element.column_id,
                        column_element.name,
                        row,
                        getColumnsContent(block.getColumnsWithTypeAndName()));
                };
                if (row == 0)
                {
                    ASSERT_EQ(column_element.column->size(), rows);
                }
                if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                {
                    if (decoding_schema->is_common_handle)
                    {
                        ASSERT_FIELD_EQ((*column_element.column)[row], Field(*std::get<0>(data_list_read[row])))
                            << gen_error_log();
                    }
                    else
                    {
                        ASSERT_FIELD_EQ((*column_element.column)[row], Field(handle_value)) << gen_error_log();
                    }
                }
                else if (column_element.name == VERSION_COLUMN_NAME)
                {
                    ASSERT_FIELD_EQ((*column_element.column)[row], Field(version_value)) << gen_error_log();
                }
                else if (column_element.name == TAG_COLUMN_NAME)
                {
                    ASSERT_FIELD_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value)))
                        << gen_error_log();
                }
                else
                {
                    if (fields_map.count(column_element.column_id) > 0)
                        ASSERT_FIELD_EQ((*column_element.column)[row], fields_map.at(column_element.column_id))
                            << gen_error_log();
                    else
                        LOG_INFO(
                            logger,
                            "ignore value check for new added column, id={}, name={}",
                            column_element.column_id,
                            column_element.name);
                }
            }
        }
    }

    bool decodeAndCheckColumns(DecodingStorageSchemaSnapshotConstPtr decoding_schema, bool force_decode) const
    {
        RegionBlockReader reader{decoding_schema};
        Block block = createBlockSortByColumnID(decoding_schema);
        if (!reader.read(block, data_list_read, force_decode))
            return false;

        checkBlock(decoding_schema, block);
        return true;
    }

    std::pair<TableInfo, std::vector<Field>> getNormalTableInfoFields(
        const ColumnIDs & pk_col_ids,
        bool is_common_handle) const
    {
        return getTableInfoAndFields(
            pk_col_ids,
            is_common_handle,
            ColumnIDValue(2, handle_value),
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
            ColumnIDValue(2, handle_value),
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
        for (auto & column : table_info.columns)
        {
            if (std::find(missing_column_ids.begin(), missing_column_ids.end(), column.id) != missing_column_ids.end())
            {
                column.origin_default_value = missing_column_default_value;
                fields_map.emplace(column.id, Field(missing_column_default_value));
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
            ColumnIDValue(2, handle_value),
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
            ColumnIDValue(2, handle_value),
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
            ColumnIDValue(2, handle_value),
            ColumnIDValue(3, std::numeric_limits<UInt64>::max()),
            ColumnIDValue(4, std::numeric_limits<Float32>::min()),
            ColumnIDValue(9, String("aaa")),
            ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
            ColumnIDValue(11, std::numeric_limits<UInt64>::min()));
        return table_info;
    }
};

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
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    ASSERT_EQ(table_info.is_common_handle, false);
    ASSERT_EQ(table_info.pk_is_handle, false);
    ASSERT_FALSE(table_info.getColumnInfo(2).hasPriKeyFlag());

    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTest, PKIsHandle)
{
    auto [table_info, fields] = getNormalTableInfoFields({2}, false);
    ASSERT_EQ(table_info.is_common_handle, false);
    ASSERT_EQ(table_info.pk_is_handle, true);
    ASSERT_TRUE(table_info.getColumnInfo(2).hasPriKeyFlag());

    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTest, CommonHandle)
{
    auto [table_info, fields] = getNormalTableInfoFields({2, 3, 4}, true);
    ASSERT_EQ(table_info.is_common_handle, true);
    ASSERT_EQ(table_info.pk_is_handle, false);
    ASSERT_TRUE(table_info.getColumnInfo(2).hasPriKeyFlag());
    ASSERT_TRUE(table_info.getColumnInfo(3).hasPriKeyFlag());
    ASSERT_TRUE(table_info.getColumnInfo(4).hasPriKeyFlag());

    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    ASSERT_TRUE(decodeAndCheckColumns(decoding_schema, true));
}

TEST_F(RegionBlockReaderTest, MissingColumnRowV2)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto new_table_info = getTableInfoWithMoreColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, false));
}

TEST_F(RegionBlockReaderTest, MissingColumnRowV1)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);
    auto new_table_info = getTableInfoWithMoreColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, false));
}

TEST_F(RegionBlockReaderTest, ExtraColumnRowV2)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);
    auto new_table_info = getTableInfoWithLessColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}

TEST_F(RegionBlockReaderTest, ExtraColumnRowV1)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);
    auto new_table_info = getTableInfoWithLessColumns({EXTRA_HANDLE_COLUMN_ID}, false);
    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}

TEST_F(RegionBlockReaderTest, OverflowColumnRowV2)
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

TEST_F(RegionBlockReaderTest, OverflowColumnRowV1)
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

TEST_F(RegionBlockReaderTest, InvalidNULLRowV2)
try
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    ASSERT_FALSE(table_info.getColumnInfo(11).hasNotNullFlag()); // col 11 is nullable

    encodeColumns(table_info, fields, RowEncodeVersion::RowV2);

    auto new_table_info = getTableInfoFieldsForInvalidNULLTest({EXTRA_HANDLE_COLUMN_ID}, false);
    ASSERT_TRUE(new_table_info.getColumnInfo(11).hasNotNullFlag()); // col 11 is not null

    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_ANY_THROW(decodeAndCheckColumns(new_decoding_schema, true));
}
CATCH

TEST_F(RegionBlockReaderTest, InvalidNULLRowV1)
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
    auto [table_info, fields] = getTableInfoAndFields(
        /*pk_col_ids*/ {3},
        false,
        ColumnIDValue<String>(3, "hello"),
        ColumnIDValueNull<String>(4));
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
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(
        /*pk_col_ids*/ {3, 4},
        false,
        ColumnIDValueNull<String>(3),
        ColumnIDValueNull<String>(4));
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
    auto [table_info, fields] = getTableInfoAndFields(
        /*pk_col_ids*/ {3},
        false,
        ColumnIDValue<String>(3, "hello"),
        ColumnIDValueNull<String>(4));
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
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(
        /*pk_col_ids*/ {3, 4},
        false,
        ColumnIDValueNull<String>(3),
        ColumnIDValueNull<String>(4));
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
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(
        /*pk_col_ids*/ {3, 4},
        false,
        ColumnIDValueNull<String>(3),
        ColumnIDValueNull<String>(4));
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
    std::tie(new_table_info, std::ignore) = getTableInfoAndFields(
        /*pk_col_ids*/ {3, 4},
        false,
        ColumnIDValueNull<String>(3),
        ColumnIDValueNull<String>(4));
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
    TableInfo table_info(
        R"({"cols":[
        {"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"case_no","O":"case_no"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":4099,"Flen":32,"Tp":15}},
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"p","O":"p"},"offset":1,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":0,"Flen":12,"Tp":15}},
        {"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"source","O":"source"},"offset":2,"origin_default":"","state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":4099,"Flen":20,"Tp":15}}
    ],"comment":"","id":77,"index_info":[],"is_common_handle":false,"name":{"L":"t_case","O":"t_case"},"partition":null,"pk_is_handle":false,"schema_version":62,"state":5,"tiflash_replica":{"Count":1},"update_timestamp":435984541435559947})",
        NullspaceID);

    RegionID region_id = 4;
    String region_start_key(bytesFromHexString("7480000000000000FF445F720000000000FA"));
    String region_end_key(bytesFromHexString("7480000000000000FF4500000000000000F8"));
    auto region = makeRegion(region_id, region_start_key, region_end_key);
    // the hex kv dump from SSTFile
    std::vector<std::tuple<std::string_view, std::string_view>> kvs = {
        {"7480000000000000FF4D5F728000000000FF0000010000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000010000000000FAF9F3126548ABFFFC",
         "508180D0BAABB3BB8606760A80000100000001010031"},
        {"7480000000000000FF4D5F728000000000FF0000020000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000020000000000FAF9F3126548ABFFFC",
         "508180D0BAABB3BB8606760A80000100000001010032"},
        {"7480000000000000FF4D5F728000000000FF0000030000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000030000000000FAF9F3126548ABFFFC",
         "508180D0BAABB3BB8606760A80000100000001010033"},
        {"7480000000000000FF4D5F728000000000FF0000040000000000FAF9F3125EFCF3FFFE", "4C8280809290B4BB8606"},
        {"7480000000000000FF4D5F728000000000FF0000040000000000FAF9F3126548ABFFFC",
         "508180D0BAABB3BB8606760A80000100000001010034"},
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

TEST_F(RegionBlockReaderTest, ReadFromRegion2)
try
{
    TableInfo table_info(
        R"json({"id":4970,"name":{"O":"customer","L":"customer"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"c_id","L":"c_id"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4099,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"c_d_id","L":"c_d_id"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4099,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":3,"name":{"O":"c_w_id","L":"c_w_id"},"offset":2,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":4107,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":4,"name":{"O":"c_first","L":"c_first"},"offset":3,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":16,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":5,"name":{"O":"c_middle","L":"c_middle"},"offset":4,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":0,"Flen":2,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":6,"name":{"O":"c_last","L":"c_last"},"offset":5,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":16,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":7,"name":{"O":"c_street_1","L":"c_street_1"},"offset":6,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":20,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":8,"name":{"O":"c_street_2","L":"c_street_2"},"offset":7,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":20,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":9,"name":{"O":"c_city","L":"c_city"},"offset":8,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":20,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":10,"name":{"O":"c_state","L":"c_state"},"offset":9,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":0,"Flen":2,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":11,"name":{"O":"c_zip","L":"c_zip"},"offset":10,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":0,"Flen":9,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":12,"name":{"O":"c_phone","L":"c_phone"},"offset":11,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":0,"Flen":16,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":13,"name":{"O":"c_since","L":"c_since"},"offset":12,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":12,"Flag":128,"Flen":19,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":14,"name":{"O":"c_credit","L":"c_credit"},"offset":13,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":254,"Flag":0,"Flen":2,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":15,"name":{"O":"c_credit_lim","L":"c_credit_lim"},"offset":14,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":246,"Flag":0,"Flen":12,"Decimal":2,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":16,"name":{"O":"c_discount","L":"c_discount"},"offset":15,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":246,"Flag":0,"Flen":4,"Decimal":4,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":17,"name":{"O":"c_balance","L":"c_balance"},"offset":16,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":246,"Flag":0,"Flen":12,"Decimal":2,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":18,"name":{"O":"c_ytd_payment","L":"c_ytd_payment"},"offset":17,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":246,"Flag":0,"Flen":12,"Decimal":2,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":19,"name":{"O":"c_payment_cnt","L":"c_payment_cnt"},"offset":18,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":20,"name":{"O":"c_delivery_cnt","L":"c_delivery_cnt"},"offset":19,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":21,"name":{"O":"c_data","L":"c_data"},"offset":20,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":500,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":48,"name":{"O":"cct_2","L":"cct_2"},"offset":21,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":3,"Flag":0,"Flen":11,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":47,"name":{"O":"adc_25","L":"adc_25"},"offset":22,"origin_default":"2023-09-12 10:01:13","origin_default_bit":null,"default":"CURRENT_TIMESTAMP","default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":12,"Flag":8320,"Flen":19,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":2,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":[{"id":1,"idx_name":{"O":"PRIMARY","L":"primary"},"tbl_name":{"O":"","L":""},"idx_cols":[{"name":{"O":"c_w_id","L":"c_w_id"},"offset":2,"length":-1},{"name":{"O":"c_d_id","L":"c_d_id"},"offset":1,"length":-1},{"name":{"O":"c_id","L":"c_id"},"offset":0,"length":-1}],"state":5,"backfill_state":0,"comment":"","index_type":1,"is_unique":true,"is_primary":true,"is_invisible":false,"is_global":false,"mv_index":false},{"id":2,"idx_name":{"O":"idx_customer","L":"idx_customer"},"tbl_name":{"O":"","L":""},"idx_cols":[{"name":{"O":"c_w_id","L":"c_w_id"},"offset":2,"length":-1},{"name":{"O":"c_d_id","L":"c_d_id"},"offset":1,"length":-1},{"name":{"O":"c_last","L":"c_last"},"offset":5,"length":-1},{"name":{"O":"c_first","L":"c_first"},"offset":3,"length":-1}],"state":5,"backfill_state":0,"comment":"","index_type":1,"is_unique":false,"is_primary":false,"is_invisible":false,"is_global":false,"mv_index":false}],"constraint_info":null,"fk_info":[],"state":5,"pk_is_handle":false,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":30207016,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":48,"max_idx_id":2,"max_fk_id":0,"max_cst_id":0,"update_timestamp":444198845243195478,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":{"type":2,"expr":"`c_w_id`","columns":[],"enable":true,"definitions":[{"id":4971,"name":{"O":"p0","L":"p0"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4972,"name":{"O":"p1","L":"p1"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4973,"name":{"O":"p2","L":"p2"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4974,"name":{"O":"p3","L":"p3"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4975,"name":{"O":"p4","L":"p4"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4976,"name":{"O":"p5","L":"p5"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4977,"name":{"O":"p6","L":"p6"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4978,"name":{"O":"p7","L":"p7"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4979,"name":{"O":"p8","L":"p8"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4980,"name":{"O":"p9","L":"p9"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4981,"name":{"O":"p10","L":"p10"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4982,"name":{"O":"p11","L":"p11"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4983,"name":{"O":"p12","L":"p12"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4984,"name":{"O":"p13","L":"p13"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4985,"name":{"O":"p14","L":"p14"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4986,"name":{"O":"p15","L":"p15"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4987,"name":{"O":"p16","L":"p16"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4988,"name":{"O":"p17","L":"p17"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4989,"name":{"O":"p18","L":"p18"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4990,"name":{"O":"p19","L":"p19"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4991,"name":{"O":"p20","L":"p20"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4992,"name":{"O":"p21","L":"p21"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4993,"name":{"O":"p22","L":"p22"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4994,"name":{"O":"p23","L":"p23"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4995,"name":{"O":"p24","L":"p24"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4996,"name":{"O":"p25","L":"p25"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4997,"name":{"O":"p26","L":"p26"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4998,"name":{"O":"p27","L":"p27"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":4999,"name":{"O":"p28","L":"p28"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5000,"name":{"O":"p29","L":"p29"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5001,"name":{"O":"p30","L":"p30"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5002,"name":{"O":"p31","L":"p31"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5003,"name":{"O":"p32","L":"p32"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5004,"name":{"O":"p33","L":"p33"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5005,"name":{"O":"p34","L":"p34"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5006,"name":{"O":"p35","L":"p35"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5007,"name":{"O":"p36","L":"p36"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5008,"name":{"O":"p37","L":"p37"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5009,"name":{"O":"p38","L":"p38"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5010,"name":{"O":"p39","L":"p39"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5011,"name":{"O":"p40","L":"p40"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5012,"name":{"O":"p41","L":"p41"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5013,"name":{"O":"p42","L":"p42"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5014,"name":{"O":"p43","L":"p43"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5015,"name":{"O":"p44","L":"p44"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5016,"name":{"O":"p45","L":"p45"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5017,"name":{"O":"p46","L":"p46"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5018,"name":{"O":"p47","L":"p47"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5019,"name":{"O":"p48","L":"p48"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5020,"name":{"O":"p49","L":"p49"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5021,"name":{"O":"p50","L":"p50"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5022,"name":{"O":"p51","L":"p51"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5023,"name":{"O":"p52","L":"p52"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5024,"name":{"O":"p53","L":"p53"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5025,"name":{"O":"p54","L":"p54"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5026,"name":{"O":"p55","L":"p55"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5027,"name":{"O":"p56","L":"p56"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5028,"name":{"O":"p57","L":"p57"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5029,"name":{"O":"p58","L":"p58"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5030,"name":{"O":"p59","L":"p59"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5031,"name":{"O":"p60","L":"p60"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5032,"name":{"O":"p61","L":"p61"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5033,"name":{"O":"p62","L":"p62"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5034,"name":{"O":"p63","L":"p63"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5035,"name":{"O":"p64","L":"p64"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5036,"name":{"O":"p65","L":"p65"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5037,"name":{"O":"p66","L":"p66"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5038,"name":{"O":"p67","L":"p67"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5039,"name":{"O":"p68","L":"p68"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5040,"name":{"O":"p69","L":"p69"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5041,"name":{"O":"p70","L":"p70"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5042,"name":{"O":"p71","L":"p71"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5043,"name":{"O":"p72","L":"p72"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5044,"name":{"O":"p73","L":"p73"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5045,"name":{"O":"p74","L":"p74"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5046,"name":{"O":"p75","L":"p75"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5047,"name":{"O":"p76","L":"p76"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5048,"name":{"O":"p77","L":"p77"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5049,"name":{"O":"p78","L":"p78"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5050,"name":{"O":"p79","L":"p79"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5051,"name":{"O":"p80","L":"p80"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5052,"name":{"O":"p81","L":"p81"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5053,"name":{"O":"p82","L":"p82"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5054,"name":{"O":"p83","L":"p83"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5055,"name":{"O":"p84","L":"p84"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5056,"name":{"O":"p85","L":"p85"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5057,"name":{"O":"p86","L":"p86"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5058,"name":{"O":"p87","L":"p87"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5059,"name":{"O":"p88","L":"p88"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5060,"name":{"O":"p89","L":"p89"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5061,"name":{"O":"p90","L":"p90"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5062,"name":{"O":"p91","L":"p91"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5063,"name":{"O":"p92","L":"p92"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5064,"name":{"O":"p93","L":"p93"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5065,"name":{"O":"p94","L":"p94"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5066,"name":{"O":"p95","L":"p95"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5067,"name":{"O":"p96","L":"p96"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5068,"name":{"O":"p97","L":"p97"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5069,"name":{"O":"p98","L":"p98"},"less_than":[],"in_values":null,"policy_ref_info":null},{"id":5070,"name":{"O":"p99","L":"p99"},"less_than":[],"in_values":null,"policy_ref_info":null}],"adding_definitions":[],"dropping_definitions":[],"NewPartitionIDs":null,"states":null,"num":100,"ddl_state":0,"new_table_id":0,"ddl_type":0,"ddl_expr":"","ddl_columns":null},"compression":"","view":null,"sequence":null,"Lock":null,"version":3,"tiflash_replica":{"Count":2,"LocationLabels":[],"Available":true,"AvailablePartitionIDs":[4987,5033,4985,4988,5007,5032,5034,5030,5059,5031,5029,5049,5028,4974,5012,5006,5011,5013,5050,5061,5024,5046,5047,4984,4989,4990,5045,4979,4986,5036,5038,5039,4996,5040,4983,5043,5017,5010,5016,5052,5018,5019,5008,5065,5009,5014,5015,5020,5026,5042,5044,5069,4972,4995,4997,4998,5023,5025,5041,5053,5054,4973,4981,4999,5000,5001,5048,4978,5051,5002,5003,5066,4991,5055,4982,5022,5056,5064,4971,4975,4976,4977,4980,4993,5037,5058,4992,4994,5004,5021,5005,5027,5035,5057,5060,5062,5063,5067,5068,5070]}})json",
        NullspaceID);

    // the hex kv dump from logging
    std::tuple<std::string_view, std::string_view> kv = {
        "7480000000000013FFA45F728000000001FF31D0DA0000000000FAF9D5E380F2DBFFBF",
        "8000160001000102030405060708090A0B0C0D0E0F101112131415302F020003000500120014001F002C003C004D004F0058006800700"
        "072007A007E0086008E008F00900067026F020B0A0691024A39644642654253754670617A4F454154494F4E4241524553454B67735173"
        "3239595A347A723063623056416D7A4474755A535562754A5131727369366762376D646555526A4A52575034383132313131313138343"
        "534323438333738383333323630000000E0D48CAC1947430C02800000C35000040488E80C027FFFFFE679F40C0280000019860B030063"
        "3732627831477346475549356D356F70596C6E57553062414F7556424E6E784D44624A366269354B7245455437564A694849647671506"
        "8366E4A5872383571394759794E73384875574B3349643369374F717A68774F793363456F454659644634684F7462624D356B436C786F"
        "386452795A7055767178687967326B363774335561686B4F455155506E59776F6C4D4E4E71454263756B57474D414A553031514B42766"
        "9756F3464504271706E5A624D72736D6B7042764B576C4E565339416A727A46307079324C746F7152303052546468374C544F48396E43"
        "7670306B6F4E4344793978436B766E47616F465735717465304F444C4E4E4B37534A70426D6F4F4E547947694C5879443334437530595"
        "A3745536479666E596E435471477565674C475779547A786B35384867623068367976626B6F444F645952786157337A43307944766849"
        "67626F7972627939624F644B4334784C426D486269676749366B383662643658505163316B30734B786E734947397636434E345065464"
        "24B394B7465686973564952757056515A4F5472745152534D6650524254376D46316C685844377953496D594C73306D61434C59796650"
        "70454E306364436D7A6F353677413063504C356D303842547779734249595A5070730000004DA018B119",
    };

    TiKVKey key(bytesFromHexString(std::get<0>(kv)));
    auto raw_key = std::get<0>(RecordKVFormat::decodeTiKVKeyFull(key));
    auto tidb_pk = RecordKVFormat::getRawTiDBPK(raw_key);
    Timestamp ts = 2;
    auto value = std::make_shared<const TiKVValue>(bytesFromHexString(std::get<1>(kv)));

    std::optional<RegionDataReadInfoList> data_list_read = std::make_optional(std::vector<RegionDataReadInfo>{
        {tidb_pk, static_cast<UInt8>(0), ts, value},
    });

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
