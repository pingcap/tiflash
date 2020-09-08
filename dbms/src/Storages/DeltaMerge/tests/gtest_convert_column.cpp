#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
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

} // namespace tests
} // namespace DM
} // namespace DB
