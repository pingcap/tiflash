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

    std::vector<std::optional<String>> strings = {};

    template <typename Integer>
    void test()
    {
        static std::vector<String> strings = {"", "abc", "www.pingcap", "中文.测.试。。。"};
        static std::vector<String> length_3_results = {"", "abc", "cap", "。。。"};

        for (size_t i = 0; i < strings.size(); ++i)
        {
            const auto & str = strings[i];
            test<Integer>(3, str, length_3_results[i]);
            test<Integer>(0, str, "");
            test<Integer>(std::numeric_limits<Integer>::max(), str, str);
            if constexpr (std::is_signed_v<Integer>)
            {
                test<Integer>(std::numeric_limits<Integer>::min(), str, "");
                test<Integer>(-1, str, "");
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
                StringRight::func_name,
                createColumn<Nullable<String>>({str, str}),
                createConstColumn<Nullable<Integer>>(2, length)));
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, result),
            executeFunction(
                StringRight::func_name,
                createConstColumn<Nullable<String>>(1, str),
                createConstColumn<Nullable<Integer>>(1, length)));
    }
};

TEST_F(StringRight, UnitTest)
try
{
    test<Int64>();
    test<UInt64>();
}
CATCH

} // namespace DB::tests
