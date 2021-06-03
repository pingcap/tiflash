#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <Storages/Transaction/TiDB.h>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
extern void setColumnDefineDefaultValue(const TiDB::TableInfo & table_info, ColumnDefine & define);

namespace tests
{

TEST(ConvertColumnType_test, CastNumeric)
{
    {
        const Strings to_types = {"UInt8", "UInt16", "UInt32", "UInt64"};

        DataTypePtr      disk_data_type = typeFromString("UInt8");
        MutableColumnPtr disk_col       = disk_data_type->createColumn();
        disk_col->insert(Field(UInt64(15)));
        disk_col->insert(Field(UInt64(255)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            UInt64 val1 = memory_column->getUInt(0);
            ASSERT_EQ(val1, 15UL);
            UInt64 val2 = memory_column->getUInt(1);
            ASSERT_EQ(val2, 255UL);
        }
    }

    {
        const Strings to_types = {"UInt16", "UInt32", "UInt64"};

        DataTypePtr      disk_data_type = typeFromString("UInt16");
        MutableColumnPtr disk_col       = disk_data_type->createColumn();
        disk_col->insert(Field(UInt64(15)));
        disk_col->insert(Field(UInt64(255)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            UInt64 val1 = memory_column->getUInt(0);
            ASSERT_EQ(val1, 15UL);
            UInt64 val2 = memory_column->getUInt(1);
            ASSERT_EQ(val2, 255UL);
        }
    }

    {
        const Strings to_types = {"UInt32", "UInt64"};

        DataTypePtr      disk_data_type = typeFromString("UInt32");
        MutableColumnPtr disk_col       = disk_data_type->createColumn();
        disk_col->insert(Field(UInt64(15)));
        disk_col->insert(Field(UInt64(255)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            UInt64 val1 = memory_column->getUInt(0);
            ASSERT_EQ(val1, 15UL);
            UInt64 val2 = memory_column->getUInt(1);
            ASSERT_EQ(val2, 255UL);
        }
    }

    {
        const Strings to_types = {"Int8", "Int16", "Int32", "Int64"};

        DataTypePtr      disk_data_type = typeFromString("Int8");
        MutableColumnPtr disk_col       = disk_data_type->createColumn();
        disk_col->insert(Field(Int64(127)));
        disk_col->insert(Field(Int64(-1)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            Int64 val1 = memory_column->getInt(0);
            ASSERT_EQ(val1, 127L);
            Int64 val2 = memory_column->getInt(1);
            ASSERT_EQ(val2, -1L);
        }
    }

    {
        const Strings to_types = {"Int16", "Int32", "Int64"};

        DataTypePtr      disk_data_type = typeFromString("Int16");
        MutableColumnPtr disk_col       = disk_data_type->createColumn();
        disk_col->insert(Field(Int64(127)));
        disk_col->insert(Field(Int64(-1)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

            Int64 val1 = memory_column->getInt(0);
            ASSERT_EQ(val1, 127L);
            Int64 val2 = memory_column->getInt(1);
            ASSERT_EQ(val2, -1L);
        }
    }

    {
        const Strings to_types = {"Int32", "Int64"};

        DataTypePtr      disk_data_type = typeFromString("Int32");
        MutableColumnPtr disk_col       = disk_data_type->createColumn();
        disk_col->insert(Field(Int64(127)));
        disk_col->insert(Field(Int64(-1)));

        for (const String & to_type : to_types)
        {
            ColumnDefine read_define(0, "c", typeFromString(to_type));
            auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

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
        disk_col->insert(Float32(3.0));
        disk_col->insert(Float32(3.1));
        disk_col->insert(Float32(3.14));
        disk_col->insert(Float32(3.141));
        disk_col->insert(Float32(3.1415));
        disk_col->insert(Float32(3.14151415141514151415141));
        disk_col->insert(Float32(-3.0));
        disk_col->insert(Float32(-3.1));
        disk_col->insert(Float32(-3.14));
        disk_col->insert(Float32(-3.141));
        disk_col->insert(Float32(-3.1415));
        disk_col->insert(Float32(-3.14151415141514151415141));
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

TEST(ConvertColumnType_test, CastNullableToNotNull)
{
    const Strings to_types = {"Int8", "Int16", "Int32", "Int64"};

    DataTypePtr      disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        read_define.default_value = Field(Int64(99));
        auto memory_column        = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

        Int64 val1 = memory_column->getInt(0);
        ASSERT_EQ(val1, 99); // "NULL" value is cast to default value
        Int64 val2 = memory_column->getInt(1);
        ASSERT_EQ(val2, 127L);
        Int64 val3 = memory_column->getUInt(2);
        ASSERT_EQ(val3, -1L);
    }
}

TEST(ConvertColumnType_test, CastNullableToNotNullWithNonZeroDefaultValue)
{
    const Strings to_types = {"Int8", "Int16", "Int32", "Int64"};

    DataTypePtr      disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        read_define.default_value = Field(Int64(5));
        auto memory_column        = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

        Int64 val1 = memory_column->getInt(0);
        ASSERT_EQ(val1, 5); // "NULL" value is cast to default value (5)
        Int64 val2 = memory_column->getInt(1);
        ASSERT_EQ(val2, 127L);
        Int64 val3 = memory_column->getUInt(2);
        ASSERT_EQ(val3, -1L);
    }
}

TEST(ConvertColumnType_test, CastNullableToNullable)
{
    const Strings to_types = {"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)"};

    DataTypePtr      disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

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

TEST(ConvertColumnType_test, CastNotNullToNullable)
{
    const Strings to_types = {"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)"};

    DataTypePtr      disk_data_type = typeFromString("Int8");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        ColumnDefine read_define(0, "c", typeFromString(to_type));
        auto         memory_column = convertColumnByColumnDefineIfNeed(disk_data_type, disk_col->getPtr(), read_define);

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

TEST(ConvertColumnType_test, GetDefaultValue)
try
{
    const String json_table_info
        = R"json({"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"a","O":"a"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":4099,"Flen":768,"Tp":15}},{"comment":"","default":"3.14","default_bit":null,"id":2,"name":{"L":"f","O":"f"},"offset":1,"origin_default":"3.14","state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":-1,"Elems":null,"Flag":0,"Flen":12,"Tp":4}},{"comment":"","default":"3.14","default_bit":null,"id":3,"name":{"L":"f2","O":"f2"},"offset":2,"origin_default":"3.14","state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":-1,"Elems":null,"Flag":1,"Flen":12,"Tp":4}}],"comment":"","id":627,"name":{"L":"t","O":"t"},"partition":null,"pk_is_handle":false,"schema_version":252,"state":5,"tiflash_replica":{"Count":0},"update_timestamp":422031263342264329})json";

    TiDB::TableInfo table_info(json_table_info);
    const auto &    columns = table_info.columns;
    EXPECT_EQ(columns.size(), 3UL);

    DM::ColumnDefine cd;

    size_t num_rows = 100;
    {
        cd.id   = 2;
        cd.type = typeFromString("Nullable(Float32)");
        DM::setColumnDefineDefaultValue(table_info, cd);
        EXPECT_EQ(cd.default_value.getType(), Field::Types::Float64);
        EXPECT_FLOAT_EQ(cd.default_value.safeGet<Float64>(), 3.14);
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
        cd.id   = 3;
        cd.type = typeFromString("Float32");
        DM::setColumnDefineDefaultValue(table_info, cd);
        EXPECT_EQ(cd.default_value.getType(), Field::Types::Float64);
        EXPECT_FLOAT_EQ(cd.default_value.safeGet<double>(), 3.14);
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
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
