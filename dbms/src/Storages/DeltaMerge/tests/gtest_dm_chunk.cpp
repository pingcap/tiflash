#include <gtest/gtest.h>

#include <Storages/DeltaMerge/Chunk.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
namespace DM
{
namespace tests
{

TEST(Chunk_test, Insert)
{
    const UInt32 num_rows = 1024;
    Chunk chunk;

    ColumnMeta u64_meta{
        1, 2, num_rows, 4,
        std::make_shared<DataTypeUInt64>()
    };
    chunk.insert(u64_meta);

    {
        const ColumnMeta &got = chunk.getColumn(u64_meta.col_id);
        EXPECT_EQ(got.col_id, u64_meta.col_id);
        EXPECT_EQ(got.rows, u64_meta.rows);
        EXPECT_EQ(got.bytes, u64_meta.bytes);
        EXPECT_EQ(got.page_id, u64_meta.page_id);
        EXPECT_TRUE(got.type->equals(*u64_meta.type));
    }

    ColumnMeta string_meta{
        2, 3, num_rows, 5,
        std::make_shared<DataTypeString>()
    };
    chunk.insert(string_meta);

    {
        const ColumnMeta &got = chunk.getColumn(string_meta.col_id);
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
    Chunk chunk;

    ColumnMeta u64_meta{
            1, 2, num_rows, 4,
            std::make_shared<DataTypeUInt64>()
    };
    chunk.insert(u64_meta);
    ColumnMeta string_meta{
            2, 3, num_rows, 5,
            std::make_shared<DataTypeString>()
    };
    chunk.insert(string_meta);
    EXPECT_FALSE(chunk.isDeleteRange());

    MemoryWriteBuffer wbuf(0, 1024);
    chunk.serialize(wbuf);
    auto buf = wbuf.buffer();
    ReadBufferFromMemory rbuf(buf.begin(), buf.size());
    Chunk deseri_chunk = Chunk::deserialize(rbuf);
    EXPECT_EQ(deseri_chunk.getRows(), chunk.getRows());
    EXPECT_FALSE(deseri_chunk.isDeleteRange());

    // check ColumnMeta in deseri_chunk
    {
        const ColumnMeta &got = deseri_chunk.getColumn(u64_meta.col_id);
        EXPECT_EQ(got.col_id, u64_meta.col_id);
        EXPECT_EQ(got.rows, u64_meta.rows);
        EXPECT_EQ(got.bytes, u64_meta.bytes);
        EXPECT_EQ(got.page_id, u64_meta.page_id);
        EXPECT_TRUE(got.type->equals(*u64_meta.type));
    }
    {
        const ColumnMeta &got = deseri_chunk.getColumn(string_meta.col_id);
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
    Chunk chunk(range);
    EXPECT_TRUE(chunk.isDeleteRange());

    MemoryWriteBuffer wbuf(0, 1024);
    chunk.serialize(wbuf);
    auto buf = wbuf.buffer();
    ReadBufferFromMemory rbuf(buf.begin(), buf.size());
    Chunk deseri_chunk = Chunk::deserialize(rbuf);
    EXPECT_TRUE(deseri_chunk.isDeleteRange());

    const HandleRange deseri_range = deseri_chunk.getDeleteRange();
    EXPECT_EQ(deseri_range.start, range.start);
    EXPECT_EQ(deseri_range.end, range.end);
}

} // namespace tests
} // namespace DM
} // namespace DB
