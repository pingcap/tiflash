#include <gtest/gtest.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/DeltaMerge/Chunk.h>

namespace DB
{
namespace DM
{
namespace tests
{

TEST(Chunk_test, Insert)
{
    const UInt32 num_rows = 1024;
    Chunk        chunk;

    ColumnMeta u64_meta{1, 2, num_rows, 4, std::make_shared<DataTypeUInt64>()};
    chunk.insert(u64_meta);

    {
        const ColumnMeta & got = chunk.getColumn(u64_meta.col_id);
        EXPECT_EQ(got.col_id, u64_meta.col_id);
        EXPECT_EQ(got.rows, u64_meta.rows);
        EXPECT_EQ(got.bytes, u64_meta.bytes);
        EXPECT_EQ(got.page_id, u64_meta.page_id);
        EXPECT_TRUE(got.type->equals(*u64_meta.type));
    }

    ColumnMeta string_meta{2, 3, num_rows, 5, std::make_shared<DataTypeString>()};
    chunk.insert(string_meta);

    {
        const ColumnMeta & got = chunk.getColumn(string_meta.col_id);
        EXPECT_EQ(got.col_id, string_meta.col_id);
        EXPECT_EQ(got.rows, string_meta.rows);
        EXPECT_EQ(got.bytes, string_meta.bytes);
        EXPECT_EQ(got.page_id, string_meta.page_id);
        EXPECT_TRUE(got.type->equals(*string_meta.type));
    }
}

TEST(Chunk_test, Seri)
{
    const UInt32 num_rows = 1024;
    Chunk        chunk;

    ColumnMeta u64_meta{1, 2, num_rows, 4, std::make_shared<DataTypeUInt64>()};
    chunk.insert(u64_meta);
    ColumnMeta string_meta{2, 3, num_rows, 5, std::make_shared<DataTypeString>()};
    chunk.insert(string_meta);
    EXPECT_FALSE(chunk.isDeleteRange());

    MemoryWriteBuffer wbuf(0, 1024);
    chunk.serialize(wbuf);
    auto                 buf = wbuf.buffer();
    ReadBufferFromMemory rbuf(buf.begin(), buf.size());
    Chunk                deseri_chunk = Chunk::deserialize(rbuf);
    EXPECT_EQ(deseri_chunk.getRows(), chunk.getRows());
    EXPECT_FALSE(deseri_chunk.isDeleteRange());

    // check ColumnMeta in deseri_chunk
    {
        const ColumnMeta & got = deseri_chunk.getColumn(u64_meta.col_id);
        EXPECT_EQ(got.col_id, u64_meta.col_id);
        EXPECT_EQ(got.rows, u64_meta.rows);
        EXPECT_EQ(got.bytes, u64_meta.bytes);
        EXPECT_EQ(got.page_id, u64_meta.page_id);
        EXPECT_TRUE(got.type->equals(*u64_meta.type));
    }
    {
        const ColumnMeta & got = deseri_chunk.getColumn(string_meta.col_id);
        EXPECT_EQ(got.col_id, string_meta.col_id);
        EXPECT_EQ(got.rows, string_meta.rows);
        EXPECT_EQ(got.bytes, string_meta.bytes);
        EXPECT_EQ(got.page_id, string_meta.page_id);
        EXPECT_TRUE(got.type->equals(*string_meta.type));
    }
}

TEST(DeleteRange_test, Seri)
{
    HandleRange range{20, 999};
    Chunk       chunk(range);
    EXPECT_TRUE(chunk.isDeleteRange());

    MemoryWriteBuffer wbuf(0, 1024);
    chunk.serialize(wbuf);
    auto                 buf = wbuf.buffer();
    ReadBufferFromMemory rbuf(buf.begin(), buf.size());
    Chunk                deseri_chunk = Chunk::deserialize(rbuf);
    EXPECT_TRUE(deseri_chunk.isDeleteRange());

    const HandleRange deseri_range = deseri_chunk.getDeleteRange();
    EXPECT_EQ(deseri_range.start, range.start);
    EXPECT_EQ(deseri_range.end, range.end);
}

namespace
{
DataTypePtr typeFromString(const String & str)
{
    auto & data_type_factory = DataTypeFactory::instance();
    return data_type_factory.get(str);
}
} // namespace

TEST(ChunkColumnCast_test, CastNumeric)
{
    {
        const Strings to_types = {"UInt8", "UInt16", "UInt32", "UInt64"};

        DataTypePtr      disk_data_type = typeFromString("UInt8");
        MutableColumnPtr disk_col       = disk_data_type->createColumn();
        disk_col->insert(Field(UInt64(15)));
        disk_col->insert(Field(UInt64(255)));

        for (const String & to_type : to_types)
        {
            DataTypePtr      read_data_type = typeFromString(to_type);
            ColumnDefine     read_define(0, "c", read_data_type);
            MutableColumnPtr memory_column = read_data_type->createColumn();
            memory_column->reserve(2);

            castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 2);

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
            DataTypePtr      read_data_type = typeFromString(to_type);
            ColumnDefine     read_define(0, "c", read_data_type);
            MutableColumnPtr memory_column = read_data_type->createColumn();
            memory_column->reserve(2);

            castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 2);

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
            DataTypePtr      read_data_type = typeFromString(to_type);
            ColumnDefine     read_define(0, "c", read_data_type);
            MutableColumnPtr memory_column = read_data_type->createColumn();
            memory_column->reserve(2);

            castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 2);

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
            DataTypePtr      read_data_type = typeFromString(to_type);
            ColumnDefine     read_define(0, "c", read_data_type);
            MutableColumnPtr memory_column = read_data_type->createColumn();
            memory_column->reserve(2);

            castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 2);

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
            DataTypePtr      read_data_type = typeFromString(to_type);
            ColumnDefine     read_define(0, "c", read_data_type);
            MutableColumnPtr memory_column = read_data_type->createColumn();
            memory_column->reserve(2);

            castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 2);

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
            DataTypePtr      read_data_type = typeFromString(to_type);
            ColumnDefine     read_define(0, "c", read_data_type);
            MutableColumnPtr memory_column = read_data_type->createColumn();
            memory_column->reserve(2);

            castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 2);

            Int64 val1 = memory_column->getInt(0);
            ASSERT_EQ(val1, 127L);
            Int64 val2 = memory_column->getInt(1);
            ASSERT_EQ(val2, -1L);
        }
    }
}

TEST(ChunkColumnCast_test, CastNullableToNotNull)
{
    const Strings to_types = {"Int16", "Int32", "Int64"};

    DataTypePtr      disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        DataTypePtr      read_data_type = typeFromString(to_type);
        ColumnDefine     read_define(0, "c", read_data_type);
        MutableColumnPtr memory_column = read_data_type->createColumn();
        memory_column->reserve(3);

        castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 3);

        Int64 val1 = memory_column->getInt(0);
        ASSERT_EQ(val1, 0); // "NULL" value is cast to 0
        Int64 val2 = memory_column->getInt(1);
        ASSERT_EQ(val2, 127L);
        Int64 val3 = memory_column->getUInt(2);
        ASSERT_EQ(val3, -1L);
    }
}

TEST(ChunkColumnCast_test, DISABLED_CastNullableToNotNullWithNonZeroDefaultValue)
{
    const Strings to_types = {"Int16", "Int32", "Int64"};

    DataTypePtr      disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        DataTypePtr  read_data_type = typeFromString(to_type);
        ColumnDefine read_define(0, "c", read_data_type);
        read_define.default_value      = "5";
        MutableColumnPtr memory_column = read_data_type->createColumn();
        memory_column->reserve(3);

        castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 3);

        Int64 val1 = memory_column->getInt(0);
        ASSERT_EQ(val1, 5); // "NULL" value is cast to default value (5)
        Int64 val2 = memory_column->getInt(1);
        ASSERT_EQ(val2, 127L);
        Int64 val3 = memory_column->getUInt(2);
        ASSERT_EQ(val3, -1L);
    }
}

TEST(ChunkColumnCast_test, CastNullableToNullable)
{
    const Strings to_types = {"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)"};

    DataTypePtr      disk_data_type = typeFromString("Nullable(Int8)");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field()); // a "NULL" value
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        DataTypePtr      read_data_type = typeFromString(to_type);
        ColumnDefine     read_define(0, "c", read_data_type);
        MutableColumnPtr memory_column = read_data_type->createColumn();
        memory_column->reserve(3);

        castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 3);

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

TEST(ChunkColumnCast_test, CastNotNullToNullable)
{
    const Strings to_types = {"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)"};

    DataTypePtr      disk_data_type = typeFromString("Int8");
    MutableColumnPtr disk_col       = disk_data_type->createColumn();
    disk_col->insert(Field(Int64(127)));
    disk_col->insert(Field(Int64(-1)));

    for (const String & to_type : to_types)
    {
        DataTypePtr      read_data_type = typeFromString(to_type);
        ColumnDefine     read_define(0, "c", read_data_type);
        MutableColumnPtr memory_column = read_data_type->createColumn();
        memory_column->reserve(2);

        castColumnAccordingToColumnDefine(disk_data_type, disk_col->getPtr(), read_define, memory_column->getPtr(), 0, 2);

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
