#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class Logical : public DB::tests::FunctionTest
{
};

TEST_F(Logical, andTest)
try
{
    const String & func_name = "and";

    // column, column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 1, 0, 0, {}, 0}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
            createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}})));
    // column, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, 0}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<UInt8>>(2, 1),
            createColumn<Nullable<UInt8>>({1, 0})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt8>>(1, 1),
        executeFunction(
            func_name,
            createConstColumn<Nullable<UInt8>>(1, 1),
            createConstColumn<Nullable<UInt8>>(1, 1)));
    // only null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({{}, 0}),
        executeFunction(
            func_name,
            createOnlyNullColumn(2),
            createColumn<Nullable<UInt8>>({1, 0})));
}
CATCH

TEST_F(Logical, orTest)
try
{
    const String & func_name = "or";

    // column, column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 1, 1, 1, 1, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
            createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}})));
    // column, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, 1}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<UInt8>>(2, 1),
            createColumn<Nullable<UInt8>>({1, 0})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt8>>(1, 1),
        executeFunction(
            func_name,
            createConstColumn<Nullable<UInt8>>(1, 1),
            createConstColumn<Nullable<UInt8>>(1, 0)));
    // only null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, {}}),
        executeFunction(
            func_name,
            createOnlyNullColumn(2),
            createColumn<Nullable<UInt8>>({1, 0})));
}
CATCH

TEST_F(Logical, xorTest)
try
{
    const String & func_name = "xor";

    // column, column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 0, 1, 1, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({0, 1, 0, 1, {}, 0}),
            createColumn<Nullable<UInt8>>({0, 1, 1, 0, 1, {}})));
    // column, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({0, 1}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<UInt8>>(2, 1),
            createColumn<Nullable<UInt8>>({1, 0})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt8>>(1, 0),
        executeFunction(
            func_name,
            createConstColumn<Nullable<UInt8>>(1, 1),
            createConstColumn<Nullable<UInt8>>(1, 1)));
    // only null
    ASSERT_COLUMN_EQ(
        createOnlyNullColumn(2),
        executeFunction(
            func_name,
            createOnlyNullColumn(2),
            createColumn<Nullable<UInt8>>({1, 0})));
}
CATCH

TEST_F(Logical, notTest)
try
{
    const String & func_name = "not";

    // column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({1, 0, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({0, 1, {}})));
    // const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt8>>(1, 0),
        executeFunction(
            func_name,
            createConstColumn<Nullable<UInt8>>(1, 1)));
    // only null
    ASSERT_COLUMN_EQ(
        createOnlyNullColumn(1),
        executeFunction(
            func_name,
            createOnlyNullColumn(1)));
}
CATCH

} // namespace DB::tests