#include <Storages/Transaction/RowCodec.h>
#include <gtest/gtest.h>

namespace DB::tests
{

using TiDB::ColumnInfo;
using TiDB::TableInfo;

template <typename T>
size_t testIntLength(T i)
{
    TableInfo ti;
    {
        ColumnInfo ci;
        ci.id = 1;
        ci.tp = TiDB::TypeLongLong;
        if constexpr (std::is_unsigned_v<T>)
            ci.setUnsignedFlag();
        ti.columns.emplace_back(std::move(ci));
    }

    std::stringstream ss;
    encodeRowV2(ti, std::vector<Field>{Field(static_cast<typename NearestFieldType<T>::Type>(i))}, ss);
    return ss.str().size() - (1 + 1 + 2 + 2 + 1 + 2);
}

TEST(RowV2Suite, IntLength)
{
    ASSERT_EQ(testIntLength(std::numeric_limits<Int8>::max()), 1UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<Int8>::min()), 1UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt8>::max()), 1UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt8>::min()), 1UL);
    ASSERT_EQ(testIntLength(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::max()) + Int16(1))), 2UL);
    ASSERT_EQ(testIntLength(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::min()) - Int16(1))), 2UL);
    ASSERT_EQ(testIntLength(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::max()) + UInt16(1))), 2UL);
    ASSERT_EQ(testIntLength(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::min()) - UInt16(1))), 2UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<Int16>::max()), 2UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<Int16>::min()), 2UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt16>::max()), 2UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt16>::min()), 1UL);
    ASSERT_EQ(testIntLength(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::max()) + Int32(1))), 4UL);
    ASSERT_EQ(testIntLength(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::min()) - Int32(1))), 4UL);
    ASSERT_EQ(testIntLength(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::max()) + UInt32(1))), 4UL);
    ASSERT_EQ(testIntLength(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::min()) - UInt32(1))), 4UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<Int32>::max()), 4UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<Int32>::min()), 4UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt32>::max()), 4UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt32>::min()), 1UL);
    ASSERT_EQ(testIntLength(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::max()) + Int64(1))), 8UL);
    ASSERT_EQ(testIntLength(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::min()) - Int64(1))), 8UL);
    ASSERT_EQ(testIntLength(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + UInt64(1))), 8UL);
    ASSERT_EQ(testIntLength(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::min()) - UInt64(1))), 8UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<Int64>::max()), 8UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<Int64>::min()), 8UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt64>::max()), 8UL);
    ASSERT_EQ(testIntLength(std::numeric_limits<UInt64>::min()), 1UL);
}

} // namespace DB::tests
