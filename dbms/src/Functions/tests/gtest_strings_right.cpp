#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class StringRight : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "rightUTF8";
};

TEST_F(StringRight, UnitTest)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", ""}),
        executeFunction(
            StringRight::func_name,
            createColumn<Nullable<String>>({"", "www.pingcap", "中文.测.试。。。", {}}),
            createColumn<Nullable<Int64>>({0, 1, 2, 3})));
}
CATCH

} // namespace DB::tests
