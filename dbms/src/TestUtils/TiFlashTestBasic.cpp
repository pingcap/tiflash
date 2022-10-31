#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
::testing::AssertionResult DataTypeCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const DataTypePtr & lhs,
    const DataTypePtr & rhs)
{
    if (lhs->equals(*rhs))
        return ::testing::AssertionSuccess();
    else
        return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs->getName(), rhs->getName(), false);
}

::testing::AssertionResult fieldCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const Field & lhs,
    const Field & rhs)
{
    if (lhs == rhs)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toString(), rhs.toString(), false);
}

} // namespace DB::tests
