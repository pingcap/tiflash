#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
namespace tests
{
class TestColumnDecimal : public ::testing::Test
{
};

template <typename Decimal>
void testCloneResized(int precision)
{
    using Native = typename Decimal::NativeType;
    using FieldType = DecimalField<Decimal>;

    auto column_ptr = createColumn<Decimal>(
        std::make_tuple(precision, 4),
        {FieldType(static_cast<Native>(0), 4),
         FieldType(static_cast<Native>(1), 4),
         FieldType(static_cast<Native>(2), 4),
         FieldType(static_cast<Native>(3), 0)}).column;
    auto clone_column_ptr = column_ptr->cloneResized(column_ptr->size() + 1);

    Field f;
    for (size_t i = 0; i != column_ptr->size(); ++i)
    {
        column_ptr->get(i, f);
        auto origin_value [[maybe_unused]] = f.template get<Decimal>();
        clone_column_ptr->get(i, f);
        auto clone_value [[maybe_unused]] = f.template get<Decimal>();
        ASSERT_TRUE(origin_value == clone_value);
    }
    clone_column_ptr->get(column_ptr->size(), f);
    auto last_value = f.template get<Decimal>();
    ASSERT_TRUE(last_value.value == 0);
}

TEST_F(TestColumnDecimal, CloneResized)
try
{
    testCloneResized<Decimal32>(9);
    testCloneResized<Decimal64>(18);
    testCloneResized<Decimal128>(38);
    testCloneResized<Decimal256>(65);
}
CATCH

}
}