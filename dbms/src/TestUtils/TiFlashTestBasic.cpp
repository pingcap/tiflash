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
} // namespace DB::tests
