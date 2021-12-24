#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <limits>
#include <string>
#include <vector>

namespace DB::tests
{
class StringRight : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "rightUTF8";

    template <typename Integer>
    void testVectorConst()
    {

    }

    template <typename Integer>
    void testConstConst()
    {

    }
};

TEST_F(StringRight, UnitTest)
try
{
    // todo test vector vector after FunctionRightUTF8 supported

    // test vector const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "gcap", "试。。。", {}}),
        executeFunction(
            StringRight::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createConstColumn<Nullable<Int64>>(4, 4)));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "gcap", "试。。。", {}}),
        executeFunction(
            StringRight::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createConstColumn<Nullable<Int64>>(4, 4)));

    // test const const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, "中文.测.试。。。"),
        executeFunction(
            StringRight::func_name,
            createConstColumn<Nullable<String>>(1, "中文.测.试。。。"),
            createConstColumn<Nullable<Int64>>(1, 2)));
}
CATCH

} // namespace DB::tests
