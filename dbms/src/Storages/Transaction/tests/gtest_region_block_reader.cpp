#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/TypeMapping.h>
#include <gtest/gtest.h>

#include "row_v2_basic_include.h"

using TableInfo = TiDB::TableInfo;

namespace DB::tests
{
static String getTestColumnName(ColumnID id) { return "column" + std::to_string(id); }

TEST(RegionBlockReader_test, pkIsNotHandle)
{
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;

    auto [table_info, _, fields] = getTableInfoLutFields(
        ColumnIDValue(1, std::numeric_limits<Int8>::max()),
        ColumnIDValue(2, std::numeric_limits<Int8>::min()),
        ColumnIDValue(3, std::numeric_limits<UInt8>::max()),
        ColumnIDValue(4, std::numeric_limits<UInt8>::min()),
        ColumnIDValue(5, std::numeric_limits<Int16>::max()),
        ColumnIDValue(6, std::numeric_limits<Int16>::min()),
        ColumnIDValue(7, std::numeric_limits<UInt16>::max()),
        ColumnIDValue(8, std::numeric_limits<UInt16>::min()),
        ColumnIDValue(9, std::numeric_limits<Int32>::max()),
        ColumnIDValue(10, std::numeric_limits<Int32>::min()),
        ColumnIDValue(11, std::numeric_limits<UInt32>::max()),
        ColumnIDValue(12, std::numeric_limits<UInt32>::min()),
        ColumnIDValue(13, std::numeric_limits<Int64>::max()),
        ColumnIDValue(14, std::numeric_limits<Int64>::min()),
        ColumnIDValue(15, std::numeric_limits<UInt64>::max()),
        ColumnIDValue(16, std::numeric_limits<UInt64>::min()),
        ColumnIDValue(17, Float32(0)),
        ColumnIDValue(18, std::numeric_limits<Float32>::min()),
        ColumnIDValue(19, std::numeric_limits<Float32>::max()),
        ColumnIDValue(20, Float64(0)),
        ColumnIDValue(21, std::numeric_limits<Float64>::min()),
        ColumnIDValue(22, std::numeric_limits<Float64>::max()),
        ColumnIDValue(23, String("")),
        ColumnIDValue(24, String("aaa")),
        ColumnIDValue(25, String(std::numeric_limits<UInt16>::max(), 'a')),
        ColumnIDValue(26, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValue(27, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5)),
        ColumnIDValue(28, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 2), 2)),
        ColumnIDValueNull<UInt64>(29)
        );

    RegionDataReadInfoList data_list_read;
    {
        // create PK
        WriteBufferFromOwnString pk_buf;
        DB::EncodeInt64(handle_value, pk_buf);
        RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};

        // create value
        WriteBufferFromOwnString value_buf;
        encodeRowV2(table_info, fields, value_buf);
        data_list_read.emplace_back(pk, del_mark_value, version_value, std::make_shared<const TiKVValue>(std::move(value_buf.str())));
    }

    DM::ColumnDefines store_columns;
    DM::ColumnDefine handle_column{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE};
    {
        store_columns.emplace_back(handle_column);
        store_columns.emplace_back(VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);
        store_columns.emplace_back(TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE);
    }
    for (auto & column_info: table_info.columns)
    {
        store_columns.emplace_back(column_info.id, getTestColumnName(column_info.id), DB::getDataTypeByColumnInfo(column_info));
    }
    auto decoding_schema_snapshot = std::make_shared<DecodingStorageSchemaSnapshot>(std::make_shared<DM::ColumnDefines>(store_columns), table_info, handle_column, false);

    constexpr size_t MustHaveCount = 3;
    RegionBlockReader reader{decoding_schema_snapshot};
    Block block = createBlockSortByColumnID(decoding_schema_snapshot);
    reader.read(block, data_list_read, true);
    {
        ASSERT_EQ(block.rows(), 1UL);
        ASSERT_EQ(block.columns(), MustHaveCount + table_info.columns.size());
        ASSERT_EQ((*block.getByName(EXTRA_HANDLE_COLUMN_NAME).column)[0], Field(handle_value));
        ASSERT_EQ((*block.getByName(VERSION_COLUMN_NAME).column)[0], Field(version_value));
        ASSERT_EQ((*block.getByName(TAG_COLUMN_NAME).column)[0], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
        for (size_t pos = MustHaveCount; pos < block.columns(); pos++)
        {
            std::cout << "decoding column id " << pos - MustHaveCount + 1 << std::endl;
            ASSERT_EQ(block.getByPosition(pos).column_id, (Int64)(pos - MustHaveCount + 1));
            ASSERT_EQ(block.getByPosition(pos).column->size(), 1UL);
            ASSERT_EQ((*block.getByPosition(pos).column)[0], fields[pos - MustHaveCount]);
        }
    }
}

TEST(RegionBlockReader_test, pkIsHandle)
{
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;

    // choose intentionally which is a int64 column below
    ColumnID handle_id = 14;
    auto [table_info, _, fields] = getTableInfoLutFields(
        ColumnIDValue(1, std::numeric_limits<Int8>::max()),
        ColumnIDValue(2, std::numeric_limits<Int8>::min()),
        ColumnIDValue(3, std::numeric_limits<UInt8>::max()),
        ColumnIDValue(4, std::numeric_limits<UInt8>::min()),
        ColumnIDValue(5, std::numeric_limits<Int16>::max()),
        ColumnIDValue(6, std::numeric_limits<Int16>::min()),
        ColumnIDValue(7, std::numeric_limits<UInt16>::max()),
        ColumnIDValue(8, std::numeric_limits<UInt16>::min()),
        ColumnIDValue(9, std::numeric_limits<Int32>::max()),
        ColumnIDValue(10, std::numeric_limits<Int32>::min()),
        ColumnIDValue(11, std::numeric_limits<UInt32>::max()),
        ColumnIDValue(12, std::numeric_limits<UInt32>::min()),
        ColumnIDValue(13, std::numeric_limits<Int64>::max()),
        ColumnIDValue(14, handle_value),
        ColumnIDValue(15, std::numeric_limits<UInt64>::max()),
        ColumnIDValue(16, std::numeric_limits<UInt64>::min()),
        ColumnIDValue(17, Float32(0)),
        ColumnIDValue(18, std::numeric_limits<Float32>::min()),
        ColumnIDValue(19, std::numeric_limits<Float32>::max()),
        ColumnIDValue(20, Float64(0)),
        ColumnIDValue(21, std::numeric_limits<Float64>::min()),
        ColumnIDValue(22, std::numeric_limits<Float64>::max()),
        ColumnIDValue(23, String("")),
        ColumnIDValue(24, String("aaa")),
        ColumnIDValue(25, String(std::numeric_limits<UInt16>::max(), 'a')),
        ColumnIDValue(26, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
        ColumnIDValue(27, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5)),
        ColumnIDValue(28, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 2), 2)),
        ColumnIDValueNull<UInt64>(29)
    );
    table_info.pk_is_handle = true;
    DM::ColumnDefines store_columns;
    DM::ColumnDefine handle_column;
    DM::ColumnDefine extra_handle_column{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE};
    {
        store_columns.emplace_back(extra_handle_column);
        store_columns.emplace_back(VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);
        store_columns.emplace_back(TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE);
    }
    for (auto & column_info: table_info.columns)
    {
        if (column_info.id == handle_id)
        {
            column_info.setPriKeyFlag();
            handle_column = DM::ColumnDefine{column_info.id, getTestColumnName(column_info.id), DB::getDataTypeByColumnInfo(column_info)};
            handle_column.is_pk = true;
            store_columns.emplace_back(handle_column);
        }
        else
        {
            store_columns.emplace_back(column_info.id, getTestColumnName(column_info.id), DB::getDataTypeByColumnInfo(column_info));
        }
    }

    std::vector<Field> fields_without_handle;
    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        auto & column_info = table_info.columns[i];
        if (!(column_info.id == handle_id))
        {
            fields_without_handle.emplace_back(fields[i]);
        }
    }
    RegionDataReadInfoList data_list_read;
    {
        // create PK
        WriteBufferFromOwnString pk_buf;
        DB::EncodeInt64(handle_value, pk_buf);
        RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};

        // create value
        WriteBufferFromOwnString value_buf;
        encodeRowV2(table_info, fields_without_handle, value_buf);
        data_list_read.emplace_back(pk, del_mark_value, version_value, std::make_shared<const TiKVValue>(std::move(value_buf.str())));
    }


    auto decoding_schema_snapshot = std::make_shared<DecodingStorageSchemaSnapshot>(std::make_shared<DM::ColumnDefines>(store_columns), table_info, handle_column, false);

    constexpr size_t MustHaveCount = 3;
    RegionBlockReader reader{decoding_schema_snapshot};
    Block block = createBlockSortByColumnID(decoding_schema_snapshot);
    reader.read(block, data_list_read, true);
    {
        ASSERT_EQ(block.rows(), 1UL);
        ASSERT_EQ(block.columns(), MustHaveCount + table_info.columns.size());
        ASSERT_EQ((*block.getByName(EXTRA_HANDLE_COLUMN_NAME).column)[0], Field(handle_value));
        ASSERT_EQ((*block.getByName(VERSION_COLUMN_NAME).column)[0], Field(version_value));
        ASSERT_EQ((*block.getByName(TAG_COLUMN_NAME).column)[0], Field(NearestFieldType<UInt8>::Type(del_mark_value)));
        for (size_t pos = MustHaveCount; pos < block.columns(); pos++)
        {
            std::cout << "decoding column id " << pos - MustHaveCount + 1 << std::endl;
            ASSERT_EQ(block.getByPosition(pos).column_id, (Int64)(pos - MustHaveCount + 1));
            ASSERT_EQ(block.getByPosition(pos).column->size(), 1UL);
            ASSERT_EQ((*block.getByPosition(pos).column)[0], fields[pos - MustHaveCount]);
        }
    }
}

}
