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
class StringLeft : public DB::tests::FunctionTest
{
public:
    // leftUTF8(str,len) = substrUTF8(str,1,len)
    static constexpr auto func_name = "substringUTF8";

    template <typename Integer>
    void test()
    {
        static std::vector<String> strings = {"", "abc", "www.pingcap", "中文.测.试。。。"};
        static std::vector<String> length_3_results = {"", "abc", "www", "中文."};
        for (size_t i = 0; i < strings.size(); ++i)
        {
            const auto & str = strings[i];
            test<Integer>(3, str, length_3_results[i]);
            test<Integer>(0, str, "");
            test<Integer>(std::numeric_limits<Integer>::max(), str, str);
            if constexpr (std::is_signed_v<Integer>)
            {
                test<Integer>(std::numeric_limits<Integer>::min(), str, "");
            }
        }
    }

private:
    template <typename Integer>
    void test(Integer length, const String & str, const String & result)
    {
        // todo test vector vector after FunctionRightUTF8 supported
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({result, result}),
            executeFunction(
                StringLeft::func_name,
                createColumn<Nullable<String>>({str, str}),
                createConstColumn<Nullable<Integer>>(2, 1),
                createConstColumn<Nullable<Integer>>(2, length)));
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, result),
            executeFunction(
                StringLeft::func_name,
                createConstColumn<Nullable<String>>(1, str),
                createConstColumn<Nullable<Integer>>(1, 1),
                createConstColumn<Nullable<Integer>>(1, length)));
    }
};

TEST_F(StringLeft, UnitTest)
try
{
    test<Int64>();
    test<UInt64>();
}
CATCH

} // namespace tests
} // namespace DB