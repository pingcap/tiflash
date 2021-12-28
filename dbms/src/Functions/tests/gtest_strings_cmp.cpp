#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
namespace tests
{
class Strcmp : public DB::tests::FunctionTest
{
};

TEST_F(Strcmp, Strcmp)
try
{
    auto test_cases = TestCasesConstructor<Int8, String, String>(
        {},
        {{-1, {"a", "b"}}, {1, {"b", "a"}}, {0, {"a", "a"}}});
    auto [expected, input] = test_cases.getResultAndInput();
    auto result = executeFunction("strcmp", input);
    ASSERT_COLUMN_EQ(expected, result);

    ASSERT_COLUMN_EQ(createColumn<Int8>({-1, 1, 0, 0}), executeFunction("strcmp", {createColumn<String>({"a", "b", "a", ""}), createColumn<String>({"b", "a", "a", ""})}));
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 1, -1, std::nullopt, std::nullopt}), executeFunction("strcmp", {createColumn<Nullable<String>>({"1", "123", "123", "123", std::nullopt}), createColumn<Nullable<String>>({"123", "1", "45", std::nullopt, "123"})}));
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1, 1, 0, std::nullopt, std::nullopt}), executeFunction("strcmp", {createColumn<Nullable<String>>({"", "123", "", "", std::nullopt}), createColumn<Nullable<String>>({"123", "", "", std::nullopt, ""})}));
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"a"}), createConstColumn<Nullable<String>>(1, "b")}));
    ASSERT_COLUMN_EQ(createConstColumn<Nullable<Int8>>(1, -1), executeFunction("strcmp", {createConstColumn<Nullable<String>>(1, "a"), createConstColumn<Nullable<String>>(1, "b")}));
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({-1}), executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({"b"})}));
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int8>>({std::nullopt}), executeFunction("strcmp", {createColumn<String>({"a"}), createColumn<Nullable<String>>({std::nullopt})}));
}
CATCH
} // namespace tests
} // namespace DB
