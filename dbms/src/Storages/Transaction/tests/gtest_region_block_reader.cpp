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

#include <Core/Field.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/tests/RowCodecTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>

using TableInfo = TiDB::TableInfo;

namespace DB::tests
{
using ColumnIDs = std::vector<ColumnID>;
class RegionBlockReaderTest : public ::testing::Test
{
public:
    RegionBlockReaderTest()
        : logger(Logger::get("RegionBlockReaderTest"))
    {}

protected:
    Int64 handle_value_ = 100;
    UInt8 del_mark_value_ = 0;
    UInt64 version_value_ = 100;
    size_t rows_ = 3;

    RegionDataReadInfoList data_list_read_;
    std::unordered_map<ColumnID, Field> fields_map_;

    LoggerPtr logger;

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

    void encodeColumns(const TableInfo & table_info, const std::vector<Field> & fields, RowEncodeVersion row_version)
    {
        // for later check
        for (size_t i = 0; i < table_info.columns.size(); i++)
            fields_map_.emplace(table_info.columns[i].id, fields[i]);

        std::vector<Field> value_encode_fields;
        std::vector<Field> key_encode_fields;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            if (table_info.is_common_handle || table_info.pk_is_handle)
            {
                if (!table_info.columns[i].hasPriKeyFlag())
                    value_encode_fields.emplace_back(fields[i]);
                else
                    key_encode_fields.emplace_back(fields[i]);
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
            auto & primary_index_info = table_info.getPrimaryIndexInfo();
            for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
            {
<<<<<<< HEAD
                size_t pk_offset = primary_index_info.idx_cols[i].offset;
                EncodeDatum(pk_fields[i], table_info.columns[pk_offset].getCodecFlag(), pk_buf);
=======
                auto idx = column_name_columns_index_map[primary_index_info.idx_cols[i].name];
                EncodeDatum(key_encode_fields[i], table_info.columns[idx].getCodecFlag(), pk_buf);
>>>>>>> aae88b120d (tests: Fix RegionBlockReaderTest helper functions (#5899))
            }
        }
        else
        {
            DB::EncodeInt64(handle_value_, pk_buf);
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
<<<<<<< HEAD
        auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
        for (size_t i = 0; i < rows_; i++)
            data_list_read_.emplace_back(pk, del_mark_value_, version_value_, row_value);
=======
        auto row_value = std::make_shared<const TiKVValue>(value_buf.releaseStr());
        for (size_t i = 0; i < rows; i++)
            data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);
>>>>>>> aae88b120d (tests: Fix RegionBlockReaderTest helper functions (#5899))
    }

    void checkBlock(DecodingStorageSchemaSnapshotConstPtr decoding_schema, const Block & block) const
    {
        ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
        for (size_t row = 0; row < rows_; row++)
        {
            for (size_t pos = 0; pos < block.columns(); pos++)
            {
<<<<<<< HEAD
                auto & column_element = block.getByPosition(pos);
=======
                const auto & column_element = block.getByPosition(pos);
                auto gen_error_log = [&]() {
                    return fmt::format(
                        "  when checking column\n    id={}, name={}, nrow={}\n  decoded block is:\n{}\n",
                        column_element.column_id,
                        column_element.name,
                        row,
                        getColumnsContent(block.getColumnsWithTypeAndName()));
                };
>>>>>>> aae88b120d (tests: Fix RegionBlockReaderTest helper functions (#5899))
                if (row == 0)
                {
                    ASSERT_EQ(column_element.column->size(), rows_);
                }
                if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                {
                    if (decoding_schema->is_common_handle)
                    {
<<<<<<< HEAD
                        ASSERT_EQ((*column_element.column)[row], Field(*std::get<0>(data_list_read_[row])));
                    }
                    else
                    {
                        ASSERT_EQ((*column_element.column)[row], Field(handle_value_));
=======
                        ASSERT_FIELD_EQ((*column_element.column)[row], Field(*std::get<0>(data_list_read[row]))) << gen_error_log();
                    }
                    else
                    {
                        ASSERT_FIELD_EQ((*column_element.column)[row], Field(handle_value)) << gen_error_log();
>>>>>>> aae88b120d (tests: Fix RegionBlockReaderTest helper functions (#5899))
                    }
                }
                else if (column_element.name == VERSION_COLUMN_NAME)
                {
<<<<<<< HEAD
                    ASSERT_EQ((*column_element.column)[row], Field(version_value_));
                }
                else if (column_element.name == TAG_COLUMN_NAME)
                {
                    ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value_)));
                }
                else
                {
                    ASSERT_EQ((*column_element.column)[row], fields_map_.at(column_element.column_id));
=======
                    ASSERT_FIELD_EQ((*column_element.column)[row], Field(version_value)) << gen_error_log();
                }
                else if (column_element.name == TAG_COLUMN_NAME)
                {
                    ASSERT_FIELD_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value))) << gen_error_log();
                }
                else
                {
                    ASSERT_FIELD_EQ((*column_element.column)[row], fields_map.at(column_element.column_id)) << gen_error_log();
>>>>>>> aae88b120d (tests: Fix RegionBlockReaderTest helper functions (#5899))
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

    std::pair<TableInfo, std::vector<Field>> getNormalTableInfoFields(const ColumnIDs & pk_col_ids, bool is_common_handle) const
    {
        return getTableInfoAndFields(
            pk_col_ids,
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

} // namespace DB::tests
