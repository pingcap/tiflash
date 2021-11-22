#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
class TestNullField : public ::testing::Test
{
};

void checkNullField(const Field & null)
{
    ASSERT_EQ(null.get<UInt8>(), static_cast<UInt64>(0));
    ASSERT_EQ(null.get<UInt16>(), static_cast<UInt64>(0));
    ASSERT_EQ(null.get<UInt32>(), static_cast<UInt64>(0));
    ASSERT_EQ(null.get<UInt64>(), static_cast<UInt64>(0));
    ASSERT_EQ(null.get<UInt128>(), static_cast<UInt64>(0));
    ASSERT_EQ(null.get<Int8>(), 0);
    ASSERT_EQ(null.get<Int16>(), 0);
    ASSERT_EQ(null.get<Int32>(), 0);
    ASSERT_EQ(null.get<Int64>(), 0);
    ASSERT_EQ(null.get<Int128>(), 0);
    ASSERT_EQ(null.get<Int256>(), 0);
    ASSERT_EQ(null.get<Float32>(), 0);
    ASSERT_EQ(null.get<Float64>(), 0);
    ASSERT_EQ(null.get<String>(), "");
    ASSERT_EQ(null.get<DecimalField<Decimal32>>(), DecimalField(Decimal32(0), 0));
    ASSERT_EQ(null.get<DecimalField<Decimal64>>(), DecimalField(Decimal64(0), 0));
    ASSERT_EQ(null.get<DecimalField<Decimal128>>(), DecimalField(Decimal128(0), 0));
    ASSERT_EQ(null.get<DecimalField<Decimal256>>(), DecimalField(Decimal256(0), 0));
}
TEST_F(TestNullField, NullField)
try
{
    const Field::Null & null = Field::Null::instance();
    /// case 1: null field from default constructor
    Field f1;
    checkNullField(f1);
    /// case 2: null field constructed from Null
    checkNullField(null);
    /// case 3: null field constructed from another null field
    Field f2(f1);
    checkNullField(f2);
    /// case 4: null field from assignment
    f2 = null;
    checkNullField(f2);
    f2 = f1;
    checkNullField(f2);
}
CATCH

} // namespace tests

} // namespace DB