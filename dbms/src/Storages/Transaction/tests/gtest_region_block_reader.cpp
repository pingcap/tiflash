#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <gtest/gtest.h>

#include "row_codec_test_basic_include.h"

using TableInfo = TiDB::TableInfo;

namespace DB::tests
{
TEST(RegionBlockReader_test, PKIsNotHandle)
{
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;
    size_t rows = 3;
    auto [table_info, fields] = getTableInfoAndFields(
        {EXTRA_HANDLE_COLUMN_ID},
        false,
        ColumnIDValue(1, std::numeric_limits<Int8>::max()),
        ColumnIDValue(2, std::numeric_limits<UInt64>::min()),
        ColumnIDValue(3, std::numeric_limits<Float32>::min()),
        ColumnIDValue(4, String("aaa")),
        ColumnIDValue(5, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValueNull<UInt64>(6));
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);

    RegionDataReadInfoList data_list_read;
    // create PK
    WriteBufferFromOwnString pk_buf;
    DB::EncodeInt64(handle_value, pk_buf);
    RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
    // create value
    WriteBufferFromOwnString value_buf;
    encodeRowV2(table_info, fields, value_buf);
    auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
    for (size_t i = 0; i < rows; i++)
        data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);

    constexpr size_t MustHaveCount = 3;
    RegionBlockReader reader{decoding_schema};
    Block block = createBlockSortByColumnID(decoding_schema);
    reader.read(block, data_list_read, true);
    ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
    for (size_t row = 0; row < rows; row++)
    {
        for (size_t pos = 0; pos < block.columns(); pos++)
        {
            auto & column_element = block.getByPosition(pos);
            if (row == 0)
            {
                ASSERT_EQ(column_element.column->size(), rows);
            }
            if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(handle_value));
            else if (column_element.name == VERSION_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(version_value));
            else if (column_element.name == TAG_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
            else
                ASSERT_EQ((*column_element.column)[row], fields[pos - MustHaveCount]);
        }
    }
}

TEST(RegionBlockReader_test, PKIsHandle)
{
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;
    size_t rows = 3;
    ColumnID handle_id = 2;
    auto [table_info, fields] = getTableInfoAndFields(
        {handle_id},
        false,
        ColumnIDValue(1, std::numeric_limits<Int8>::max()),
        ColumnIDValue(2, handle_value),
        ColumnIDValue(3, std::numeric_limits<Float32>::min()),
        ColumnIDValue(4, String("aaa")),
        ColumnIDValue(5, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValueNull<UInt64>(6));
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);

    std::vector<Field> value_fields;
    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        if (!table_info.columns[i].hasPriKeyFlag())
            value_fields.emplace_back(fields[i]);
    }

    RegionDataReadInfoList data_list_read;
    // create PK
    WriteBufferFromOwnString pk_buf;
    DB::EncodeInt64(handle_value, pk_buf);
    RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
    // create value
    WriteBufferFromOwnString value_buf;
    encodeRowV2(table_info, value_fields, value_buf);
    auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
    for (size_t i = 0; i < rows; i++)
        data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);

    constexpr size_t MustHaveCount = 3;
    RegionBlockReader reader{decoding_schema};
    Block block = createBlockSortByColumnID(decoding_schema);
    reader.read(block, data_list_read, true);
    ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
    for (size_t row = 0; row < rows; row++)
    {
        for (size_t pos = 0; pos < block.columns(); pos++)
        {
            auto & column_element = block.getByPosition(pos);
            if (row == 0)
            {
                ASSERT_EQ(column_element.column->size(), rows);
            }
            if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(handle_value));
            else if (column_element.name == VERSION_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(version_value));
            else if (column_element.name == TAG_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
            else
                ASSERT_EQ((*column_element.column)[row], fields[pos - MustHaveCount]);
        }
    }
}

TEST(RegionBlockReader_test, CommonHandle)
{
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;
    size_t rows = 3;

    ColumnIDs handle_ids{2, 3, 4};
    auto [table_info, fields] = getTableInfoAndFields(
        handle_ids,
        true,
        ColumnIDValue(1, std::numeric_limits<Int8>::max()),
        ColumnIDValue(2, std::numeric_limits<UInt64>::min()),
        ColumnIDValue(3, std::numeric_limits<Float32>::min()),
        ColumnIDValue(4, String("aaa")),
        ColumnIDValue(5, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValueNull<UInt64>(6));
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);

    std::vector<Field> value_fields;
    std::vector<Field> pk_fields;
    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        if (!table_info.columns[i].hasPriKeyFlag())
            value_fields.emplace_back(fields[i]);
        else
            pk_fields.emplace_back(fields[i]);
    }

    RegionDataReadInfoList data_list_read;
    // create PK
    WriteBufferFromOwnString pk_buf;
    auto & primary_index_info = table_info.getPrimaryIndexInfo();
    for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
    {
        size_t pk_offset = primary_index_info.idx_cols[i].offset;
        EncodeDatum(pk_fields[i], table_info.columns[pk_offset].getCodecFlag(), pk_buf);
    }
    RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
    // create value
    WriteBufferFromOwnString value_buf;
    encodeRowV2(table_info, value_fields, value_buf);
    auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
    for (size_t i = 0; i < rows; i++)
        data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);

    constexpr size_t MustHaveCount = 3;
    RegionBlockReader reader{decoding_schema};
    Block block = createBlockSortByColumnID(decoding_schema);
    reader.read(block, data_list_read, true);
    ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
    for (size_t row = 0; row < rows; row++)
    {
        for (size_t pos = 0; pos < block.columns(); pos++)
        {
            auto & column_element = block.getByPosition(pos);

            if (row == 0)
            {
                ASSERT_EQ(column_element.column->size(), rows);
            }
            if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(*pk));
            else if (column_element.name == VERSION_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(version_value));
            else if (column_element.name == TAG_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
            else
                ASSERT_EQ((*column_element.column)[row], fields[pos - MustHaveCount]);
        }
    }
}

TEST(RegionBlockReader_test, MissingColumn)
{
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;
    size_t rows = 3;

    TableInfo table_info;
    std::vector<Field> fields;
    std::tie(table_info, fields) = getTableInfoAndFields(
        {EXTRA_HANDLE_COLUMN_ID},
        false,
        ColumnIDValue(2, std::numeric_limits<Int8>::max()),
        ColumnIDValue(3, std::numeric_limits<UInt64>::min()),
        ColumnIDValue(4, std::numeric_limits<Float32>::min()),
        ColumnIDValue(9, String("aaa")),
        ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValueNull<UInt64>(11));
    RegionDataReadInfoList data_list_read;
    // create PK
    WriteBufferFromOwnString pk_buf;
    DB::EncodeInt64(handle_value, pk_buf);
    RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
    // create value
    WriteBufferFromOwnString value_buf;
    encodeRowV2(table_info, fields, value_buf);
    auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
    for (size_t i = 0; i < rows; i++)
        data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);

    std::tie(table_info, std::ignore) = getTableInfoAndFields(
        {EXTRA_HANDLE_COLUMN_ID},
        false,
        ColumnIDValue(1, String("")),
        ColumnIDValue(2, std::numeric_limits<Int8>::max()),
        ColumnIDValue(3, std::numeric_limits<UInt64>::min()),
        ColumnIDValue(4, std::numeric_limits<Float32>::min()),
        ColumnIDValue(8, String("")),
        ColumnIDValue(9, String("aaa")),
        ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValueNull<UInt64>(11),
        ColumnIDValue(13, String("")));
    // add default value for missing column
    std::vector<ColumnID> missing_column_ids{1, 8, 13};
    String missing_column_default_value = String("default");
    std::unordered_map<ColumnID, Field> fields_map;
    size_t next_field_index = 0;
    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        if (std::find(missing_column_ids.begin(), missing_column_ids.end(), table_info.columns[i].id) != missing_column_ids.end())
            table_info.columns[i].origin_default_value = missing_column_default_value;
        else
            fields_map.emplace(table_info.columns[i].id, fields[next_field_index++]);
    }
    DM::ColumnDefine extra_handle_column{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE};
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);

    RegionBlockReader reader{decoding_schema};
    Block block = createBlockSortByColumnID(decoding_schema);
    bool ok = reader.read(block, data_list_read, false);
    ASSERT_EQ(ok, true);
    ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
    for (size_t row = 0; row < rows; row++)
    {
        for (size_t pos = 0; pos < block.columns(); pos++)
        {
            auto & column_element = block.getByPosition(pos);

            if (row == 0)
            {
                ASSERT_EQ(column_element.column->size(), rows);
            }
            std::cout << "checking column " << column_element.name << std::endl;
            if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(handle_value));
            else if (column_element.name == VERSION_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(version_value));
            else if (column_element.name == TAG_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
            else if (std::find(missing_column_ids.begin(), missing_column_ids.end(), column_element.column_id) != missing_column_ids.end())
                ASSERT_EQ((*column_element.column)[row], Field(missing_column_default_value));
            else
                ASSERT_EQ((*column_element.column)[row], fields_map[column_element.column_id]);
        }
    }
}

TEST(RegionBlockReader_test, ExtraColumn)
{
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;
    size_t rows = 3;

    TableInfo table_info;
    std::vector<Field> fields;
    std::tie(table_info, fields) = getTableInfoAndFields(
        {EXTRA_HANDLE_COLUMN_ID},
        false,
        ColumnIDValue(2, std::numeric_limits<Int8>::max()),
        ColumnIDValue(3, std::numeric_limits<UInt64>::min()),
        ColumnIDValue(4, std::numeric_limits<Float32>::min()),
        ColumnIDValue(9, String("aaa")),
        ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValueNull<UInt64>(11));
    std::unordered_map<ColumnID, Field> fields_map;
    for (size_t i = 0; i < table_info.columns.size(); i++)
        fields_map.emplace(table_info.columns[i].id, fields[i]);
    RegionDataReadInfoList data_list_read;
    // create PK
    WriteBufferFromOwnString pk_buf;
    DB::EncodeInt64(handle_value, pk_buf);
    RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
    // create value
    WriteBufferFromOwnString value_buf;
    encodeRowV2(table_info, fields, value_buf);
    auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
    for (size_t i = 0; i < rows; i++)
        data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);

    std::tie(table_info, std::ignore) = getTableInfoAndFields(
        {EXTRA_HANDLE_COLUMN_ID},
        false,
        ColumnIDValue(2, std::numeric_limits<Int8>::max()),
        ColumnIDValue(4, std::numeric_limits<Float32>::min()),
        ColumnIDValue(9, String("aaa")),
        ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValueNull<UInt64>(11));
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    std::vector<ColumnID> extra_column_ids{3};

    RegionBlockReader reader{decoding_schema};
    Block block = createBlockSortByColumnID(decoding_schema);
    bool ok = reader.read(block, data_list_read, false);
    clearBlockData(block);
    ASSERT_EQ(ok, false);
    ok = reader.read(block, data_list_read, true);
    ASSERT_EQ(ok, true);
    ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
    for (size_t row = 0; row < rows; row++)
    {
        for (size_t pos = 0; pos < block.columns(); pos++)
        {
            auto & column_element = block.getByPosition(pos);

            if (row == 0)
            {
                ASSERT_EQ(column_element.column->size(), rows);
            }
            std::cout << "checking column " << column_element.name << std::endl;
            if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(handle_value));
            else if (column_element.name == VERSION_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(version_value));
            else if (column_element.name == TAG_COLUMN_NAME)
                ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
            else
                ASSERT_EQ((*column_element.column)[row], fields_map[column_element.column_id]);
        }
    }
}

TEST(RegionBlockReader_test, OverflowColumn)
{
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;
    size_t rows = 3;
    auto [table_info, fields] = getTableInfoAndFields(
        {EXTRA_HANDLE_COLUMN_ID},
        false,
        ColumnIDValue(1, std::numeric_limits<UInt64>::max()));
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);

    RegionDataReadInfoList data_list_read;
    // create PK
    WriteBufferFromOwnString pk_buf;
    DB::EncodeInt64(handle_value, pk_buf);
    RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
    // create value
    WriteBufferFromOwnString value_buf;
    encodeRowV2(table_info, fields, value_buf);
    auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
    for (size_t i = 0; i < rows; i++)
        data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);

    TableInfo narrow_table_info;
    std::tie(narrow_table_info, std::ignore) = getTableInfoAndFields(
        {EXTRA_HANDLE_COLUMN_ID},
        false,
        ColumnIDValue(1, std::numeric_limits<UInt8>::min()));
    auto narrow_decoding_schema = getDecodingStorageSchemaSnapshot(narrow_table_info);

    constexpr size_t MustHaveCount = 3;
    {
        RegionBlockReader reader{narrow_decoding_schema};
        Block block = createBlockSortByColumnID(narrow_decoding_schema);
        bool ok = reader.read(block, data_list_read, false);
        ASSERT_EQ(ok, false);
    }
    {
        RegionBlockReader reader{decoding_schema};
        Block block = createBlockSortByColumnID(decoding_schema);
        bool ok = reader.read(block, data_list_read, true);
        ASSERT_EQ(ok, true);
        ASSERT_EQ(block.columns(), decoding_schema->column_defines->size());
        for (size_t row = 0; row < rows; row++)
        {
            for (size_t pos = 0; pos < block.columns(); pos++)
            {
                auto & column_element = block.getByPosition(pos);
                if (row == 0)
                {
                    ASSERT_EQ(column_element.column->size(), rows);
                }
                if (column_element.name == EXTRA_HANDLE_COLUMN_NAME)
                    ASSERT_EQ((*column_element.column)[row], Field(handle_value));
                else if (column_element.name == VERSION_COLUMN_NAME)
                    ASSERT_EQ((*column_element.column)[row], Field(version_value));
                else if (column_element.name == TAG_COLUMN_NAME)
                    ASSERT_EQ((*column_element.column)[row], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
                else
                    ASSERT_EQ((*column_element.column)[row], fields[pos - MustHaveCount]);
            }
        }
    }
}

} // namespace DB::tests
