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

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/SchemaUpdate.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::DM::tests
{

TEST(ConvertColumnTypeTest, CastNumeric)
{
    {
        const Strings to_types = {"UInt8", "UInt16", "UInt32", "UInt64"};

        DataTypePtr disk_data_type = typeFromString("UInt8");
        MutableColumnPtr disk_col = disk_data_type->createColumn();
        disk_col->insert(Field(static_cast<UInt64>(15)));
        disk_col->insert(Field(static_cast<UInt64>(255)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            UInt64 val1 = memory_column->getUInt(0);
            ASSERT_EQ(val1, 15UL);
            UInt64 val2 = memory_column->getUInt(1);
            ASSERT_EQ(val2, 255UL);
        }
    }

    {
        const Strings to_types = {"UInt16", "UInt32", "UInt64"};

        DataTypePtr disk_data_type = typeFromString("UInt16");
        MutableColumnPtr disk_col = disk_data_type->createColumn();
        disk_col->insert(Field(static_cast<UInt64>(15)));
        disk_col->insert(Field(static_cast<UInt64>(255)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            UInt64 val1 = memory_column->getUInt(0);
            ASSERT_EQ(val1, 15UL);
            UInt64 val2 = memory_column->getUInt(1);
            ASSERT_EQ(val2, 255UL);
        }
    }

    {
        const Strings to_types = {"UInt32", "UInt64"};

        DataTypePtr disk_data_type = typeFromString("UInt32");
        MutableColumnPtr disk_col = disk_data_type->createColumn();
        disk_col->insert(Field(static_cast<UInt64>(15)));
        disk_col->insert(Field(static_cast<UInt64>(255)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            UInt64 val1 = memory_column->getUInt(0);
            ASSERT_EQ(val1, 15UL);
            UInt64 val2 = memory_column->getUInt(1);
            ASSERT_EQ(val2, 255UL);
        }
    }

    {
        const Strings to_types = {"Int8", "Int16", "Int32", "Int64"};

        DataTypePtr disk_data_type = typeFromString("Int8");
        MutableColumnPtr disk_col = disk_data_type->createColumn();
        disk_col->insert(Field(static_cast<Int64>(127)));
        disk_col->insert(Field(static_cast<Int64>(-1)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            Int64 val1 = memory_column->getInt(0);
            ASSERT_EQ(val1, 127L);
            Int64 val2 = memory_column->getInt(1);
            ASSERT_EQ(val2, -1L);
        }
    }

    {
        const Strings to_types = {"Int16", "Int32", "Int64"};

        DataTypePtr disk_data_type = typeFromString("Int16");
        MutableColumnPtr disk_col = disk_data_type->createColumn();
        disk_col->insert(Field(static_cast<Int64>(127)));
        disk_col->insert(Field(static_cast<Int64>(-1)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            Int64 val1 = memory_column->getInt(0);
            ASSERT_EQ(val1, 127L);
            Int64 val2 = memory_column->getInt(1);
            ASSERT_EQ(val2, -1L);
        }
    }

    {
        const Strings to_types = {"Int32", "Int64"};

        DataTypePtr disk_data_type = typeFromString("Int32");
        MutableColumnPtr disk_col = disk_data_type->createColumn();
        disk_col->insert(Field(static_cast<Int64>(127)));
        disk_col->insert(Field(static_cast<Int64>(-1)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            Int64 val1 = memory_column->getInt(0);
            ASSERT_EQ(val1, 127L);
            Int64 val2 = memory_column->getInt(1);
            ASSERT_EQ(val2, -1L);
        }
    }

    {
        const Strings to_types = {"Float64"};
        DataTypePtr disk_data_type = typeFromString("Float32");
        auto disk_col = ColumnFloat32::create();
        disk_col->insert(static_cast<Float32>(3.0));
        disk_col->insert(static_cast<Float32>(3.1));
        disk_col->insert(static_cast<Float32>(3.14));
        disk_col->insert(static_cast<Float32>(3.141));
        disk_col->insert(static_cast<Float32>(3.1415));
        disk_col->insert(static_cast<Float32>(3.14151415141514151415141));
        disk_col->insert(static_cast<Float32>(-3.0));
        disk_col->insert(static_cast<Float32>(-3.1));
        disk_col->insert(static_cast<Float32>(-3.14));
        disk_col->insert(static_cast<Float32>(-3.141));
        disk_col->insert(static_cast<Float32>(-3.1415));
        disk_col->insert(static_cast<Float32>(-3.14151415141514151415141));
        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto mem_col = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            for (size_t i = 0; i < disk_col->size(); i++)
            {
                Field f64 = (*mem_col)[i];
                EXPECT_FLOAT_EQ(disk_col->getElement(i), static_cast<Float32>(f64.get<Float64>()));
            }
        }
    }
}

TEST(ConvertColumnTypeTest, CastMyDateTime)
try
{
    {
        const Strings to_types = {"MyDateTime(1)", "MyDateTime(3)", "MyDateTime(6)"};

        DataTypePtr disk_data_type = typeFromString("MyDateTime(0)");
        MutableColumnPtr disk_col = disk_data_type->createColumn();
        disk_col->insert(Field(static_cast<UInt64>(MyDateTime(2023, 7, 17, 15, 39, 29, 20).toPackedUInt())));
        disk_col->insert(Field(static_cast<UInt64>(MyDateTime(2020, 2, 29, 15, 39, 29, 20).toPackedUInt())));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            UInt64 val1 = memory_column->getUInt(0);
            MyDateTime myval(val1);
            EXPECT_EQ(myval.year, 2023);
            EXPECT_EQ(myval.month, 7);
            EXPECT_EQ(myval.day, 17);
            EXPECT_EQ(myval.hour, 15);
            EXPECT_EQ(myval.minute, 39);
            EXPECT_EQ(myval.second, 29);
            EXPECT_EQ(myval.micro_second, 20);

            UInt64 val2 = memory_column->getUInt(1);
            MyDateTime myval2(val2);
            EXPECT_EQ(myval2.year, 2020);
            EXPECT_EQ(myval2.month, 2);
            EXPECT_EQ(myval2.day, 29);
            EXPECT_EQ(myval2.hour, 15);
            EXPECT_EQ(myval2.minute, 39);
            EXPECT_EQ(myval2.second, 29);
        }
    }
}
CATCH

TEST(ConvertColumnTypeTest, CastNullableToNotNull)
{
    const Strings to_types = {"Int8", "Int16", "Int32", "Int64"};

    DataTypePtr disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(static_cast<Int64>(127)));
    disk_col->insert(Field(static_cast<Int64>(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        read_define.default_value = Field(static_cast<Int64>(99));
        auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

        Int64 val1 = memory_column->getInt(0);
        ASSERT_EQ(val1, 99); // "NULL" value is cast to default value
        Int64 val2 = memory_column->getInt(1);
        ASSERT_EQ(val2, 127L);
        Int64 val3 = memory_column->getUInt(2);
        ASSERT_EQ(val3, -1L);
    }
}

TEST(ConvertColumnTypeTest, CastNullableToNotNullWithNonZeroDefaultValue)
{
    const Strings to_types = {"Int8", "Int16", "Int32", "Int64"};

    DataTypePtr disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(static_cast<Int64>(127)));
    disk_col->insert(Field(static_cast<Int64>(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        read_define.default_value = Field(static_cast<Int64>(5));
        auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

        Int64 val1 = memory_column->getInt(0);
        ASSERT_EQ(val1, 5); // "NULL" value is cast to default value (5)
        Int64 val2 = memory_column->getInt(1);
        ASSERT_EQ(val2, 127L);
        Int64 val3 = memory_column->getUInt(2);
        ASSERT_EQ(val3, -1L);
    }
}

TEST(ConvertColumnTypeTest, CastNullableToNullable)
{
    const Strings to_types = {"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)"};

    DataTypePtr disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(static_cast<Int64>(127)));
    disk_col->insert(Field(static_cast<Int64>(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

        ASSERT_TRUE(memory_column->isNullAt(0));
        Field f = (*memory_column)[0];
        ASSERT_TRUE(f.isNull());

        ASSERT_FALSE(memory_column->isNullAt(1));
        f = (*memory_column)[1];
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), 127L);

        ASSERT_FALSE(memory_column->isNullAt(2));
        f = (*memory_column)[2];
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), -1L);
    }
}

TEST(ConvertColumnTypeTest, CastNotNullToNullable)
{
    const Strings to_types = {"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)"};

    DataTypePtr disk_data_type = typeFromString("Int8");
    MutableColumnPtr disk_col = disk_data_type->createColumn();
    disk_col->insert(Field(static_cast<Int64>(127)));
    disk_col->insert(Field(static_cast<Int64>(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        auto memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

        ASSERT_FALSE(memory_column->isNullAt(0));
        Field f = (*memory_column)[0];
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), 127L);

        ASSERT_FALSE(memory_column->isNullAt(1));
        f = (*memory_column)[1];
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), -1L);
    }
}

TEST(ConvertColumnTypeTest, GetDefaultValue)
try
{
    const String json_table_info = R"json({
"cols":[
    {"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"a","O":"a"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":4099,"Flen":768,"Tp":15}}
    ,{"comment":"","default":"3.14","default_bit":null,"id":2,"name":{"L":"f","O":"f"},"offset":1,"origin_default":"3.14","state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":-1,"Elems":null,"Flag":0,"Flen":12,"Tp":4}}
    ,{"comment":"","default":"3.14","default_bit":null,"id":3,"name":{"L":"f2","O":"f2"},"offset":2,"origin_default":"3.14","state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":-1,"Elems":null,"Flag":1,"Flen":12,"Tp":4}}
    ,{"comment":"","default":"-5.4999999","default_bit":null,"id":4,"name":{"L":"d","O":"d"},"offset":2,"origin_default":"-5.4999999","state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":1,"Tp":246}}
    ,{"comment":"","default":"0.050000001","default_bit":null,"id":5,"name":{"L":"d2","O":"d2"},"offset":3,"origin_default":"0.050000001","state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":1,"Elems":null,"Flag":0,"Flen":1,"Tp":246}}
]
,"comment":"","id":627,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":false,"schema_version":252,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":422031263342264329
})json";

    TiDB::TableInfo table_info(json_table_info, NullspaceID);
    const auto & columns = table_info.columns;
    EXPECT_EQ(columns.size(), 5);

    DM::ColumnDefine cd;

    size_t num_rows = 100;
    {
        const auto & table_col = columns[1]; // A nullable float32 column with default value 3.14
        cd.id = table_col.id;
        EXPECT_EQ(cd.id, 2);
        cd.type = getDataTypeByColumnInfo(table_col);
        ASSERT_TRUE(cd.type->equals(*typeFromString("Nullable(Float32)")));
        // Get the default value in `ColumnDefine` set by table info
        DM::setColumnDefineDefaultValue(table_info, cd);
        EXPECT_EQ(cd.default_value.getType(), Field::Types::Float64);
        EXPECT_FLOAT_EQ(cd.default_value.safeGet<Float64>(), 3.14);
        // Try to create column by the default value
        auto col = createColumnWithDefaultValue(cd, num_rows);
        ASSERT_EQ(col->size(), num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col)[i];
            EXPECT_FLOAT_EQ(f.get<Float64>(), 3.14);
        }
        // Try to copy using inserRangeFrom
        auto col2 = cd.type->createColumn();
        col2->insertRangeFrom(*col, 0, num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col2)[i];
            EXPECT_FLOAT_EQ(f.get<Float64>(), 3.14);
        }
    }

    {
        const auto & table_col = columns[2]; // A not-null float32 column with default value 3.14
        cd.id = table_col.id;
        EXPECT_EQ(cd.id, 3);
        cd.type = getDataTypeByColumnInfo(table_col);
        ASSERT_TRUE(cd.type->equals(*typeFromString("Float32")));
        // Get the default value in `ColumnDefine` set by table info
        DM::setColumnDefineDefaultValue(table_info, cd);
        EXPECT_EQ(cd.default_value.getType(), Field::Types::Float64);
        EXPECT_FLOAT_EQ(cd.default_value.safeGet<double>(), 3.14);
        // Try to create column by the default value
        auto col = createColumnWithDefaultValue(cd, num_rows);
        ASSERT_EQ(col->size(), num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col)[i];
            EXPECT_FLOAT_EQ(f.get<Float64>(), 3.14);
        }
        // Try to copy using inserRangeFrom
        auto col2 = cd.type->createColumn();
        col2->insertRangeFrom(*col, 0, num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col2)[i];
            EXPECT_FLOAT_EQ(f.get<Float64>(), 3.14);
        }
    }

    {
        const auto & table_col = columns[3]; // A nullable Decimal(1,0) column with default value '-5.4999999' -> -5
        cd.id = table_col.id;
        EXPECT_EQ(cd.id, 4);
        cd.type = getDataTypeByColumnInfo(table_col);
        ASSERT_TRUE(cd.type->equals(*typeFromString("Nullable(Decimal(1,0))")));
        Decimal32 expected_default_value;
        {
            String s("-5.4999999");
            ReadBufferFromString buf(s);
            readDecimalText(expected_default_value, buf, /*precision*/ 1, /*scale*/ 0);
            DecimalField<Decimal32> expected_default_field(expected_default_value, /*scale*/ 0);
            EXPECT_EQ(expected_default_field.toString(), "-5");
        }
        // Get the default value in `ColumnDefine` set by table info
        DM::setColumnDefineDefaultValue(table_info, cd);
        EXPECT_EQ(cd.default_value.getType(), Field::Types::Decimal32);
        auto dec_field = cd.default_value.safeGet<DecimalField<Decimal32>>();
        EXPECT_EQ(dec_field.toString(), "-5");
        // Try to create column by the default value
        auto col = createColumnWithDefaultValue(cd, num_rows);
        ASSERT_EQ(col->size(), num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col)[i];
            EXPECT_FLOAT_EQ(f.get<DecimalField<Decimal32>>(), expected_default_value);
        }
        // Try to copy using inserRangeFrom
        auto col2 = cd.type->createColumn();
        col2->insertRangeFrom(*col, 0, num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col2)[i];
            EXPECT_FLOAT_EQ(f.get<DecimalField<Decimal32>>(), expected_default_value);
        }
    }

    {
        const auto & table_col = columns[4]; // A nullable Decimal(1,1) column with default value '0.050000001' -> 0.1
        cd.id = table_col.id;
        EXPECT_EQ(cd.id, 5);
        cd.type = getDataTypeByColumnInfo(table_col);
        ASSERT_TRUE(cd.type->equals(*typeFromString("Nullable(Decimal(1,1))")));
        Decimal32 expected_default_value;
        {
            String s("0.050000001");
            ReadBufferFromString buf(s);
            readDecimalText(expected_default_value, buf, /*precision*/ 1, /*scale*/ 1);
            DecimalField<Decimal32> expected_default_field(expected_default_value, /*scale*/ 1);
            EXPECT_EQ(expected_default_field.toString(), "0.1");
        }
        // Get the default value in `ColumnDefine` set by table info
        DM::setColumnDefineDefaultValue(table_info, cd);
        EXPECT_EQ(cd.default_value.getType(), Field::Types::Decimal32);
        auto dec_field = cd.default_value.safeGet<DecimalField<Decimal32>>();
        EXPECT_EQ(dec_field.toString(), "0.1");
        // Try to create column by the default value
        auto col = createColumnWithDefaultValue(cd, num_rows);
        ASSERT_EQ(col->size(), num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col)[i];
            EXPECT_FLOAT_EQ(f.get<DecimalField<Decimal32>>(), DecimalField(expected_default_value, /*scale*/ 1));
        }
        // Try to copy using inserRangeFrom
        auto col2 = cd.type->createColumn();
        col2->insertRangeFrom(*col, 0, num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field f = (*col2)[i];
            EXPECT_FLOAT_EQ(f.get<DecimalField<Decimal32>>(), DecimalField(expected_default_value, /*scale*/ 1));
        }
    }
}
CATCH

} // namespace DB::DM::tests
