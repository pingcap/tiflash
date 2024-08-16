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
#include <Storages/KVStore/tests/region_helper.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Schema/TiDB.h>
#include <TiDB/tests/RowCodecTestUtils.h>
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
    std::unordered_set<ColumnID> invalid_null_column_ids;

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
                    if (fields_map.contains(column_element.column_id))
                    {
                        if (!invalid_null_column_ids.contains(column_element.column_id))
                        {
                            ASSERT_FIELD_EQ((*column_element.column)[row], fields_map.at(column_element.column_id))
                                << gen_error_log();
                        }
                        else
                        {
                            ASSERT_FIELD_EQ((*column_element.column)[row], UInt64(0));
                        }
                    }
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
    invalid_null_column_ids.emplace(11);
    ASSERT_TRUE(new_table_info.getColumnInfo(11).hasNotNullFlag()); // col 11 is not null

    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
}
CATCH

TEST_F(RegionBlockReaderTest, InvalidNULLRowV1)
{
    auto [table_info, fields] = getNormalTableInfoFields({EXTRA_HANDLE_COLUMN_ID}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV1);

    auto new_table_info = getTableInfoFieldsForInvalidNULLTest({EXTRA_HANDLE_COLUMN_ID}, false);
    invalid_null_column_ids.emplace(11);
    ASSERT_TRUE(new_table_info.getColumnInfo(11).hasNotNullFlag()); // col 11 is not null

    auto new_decoding_schema = getDecodingStorageSchemaSnapshot(new_table_info);
    ASSERT_FALSE(decodeAndCheckColumns(new_decoding_schema, false));
    ASSERT_TRUE(decodeAndCheckColumns(new_decoding_schema, true));
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


} // namespace DB::tests
