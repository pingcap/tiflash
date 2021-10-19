#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class SubString : public DB::tests::FunctionTest
{
};

TEST_F(SubString, subStringUTF8Test)
try
{
    // column, const, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.", "ww.pi", "w.pi", ".pin"}),
        executeFunction(
            "substringUTF8",
            createColumn<Nullable<String>>({"www.pingcap.com", "ww.pingcap.com", "w.pingcap.com", ".pingcap.com"}),
            createConstColumn<Nullable<Int16>>(4, 1),
            createConstColumn<Nullable<Int16>>(4, 4)));
    // const, const, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www."}),
        executeFunction(
            "substringUTF8",
            createConstColumn<Nullable<String>>(1, "www.pingcap.com"),
            createConstColumn<Nullable<Int16>>(1, 1),
            createConstColumn<Nullable<Int16>>(1, 4)));
    // Test Null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, "www."}),
        executeFunction(
            "substringIndex",
            createColumn<Nullable<String>>(
                {{}, "www.pingcap.com"}),
                createConstColumn<Nullable<Int16>>(2, 1),
                createConstColumn<Nullable<Int16>>(2, 4)));
}
CATCH

} // namespace tests
} // namespace DB