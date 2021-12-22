#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
namespace DB
{
namespace tests
{
class StringCharLength : public FunctionTest
{
protected:
    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<UInt64>> & v)
    {
        return createColumn<Nullable<UInt64>>(v);
    }

    static ColumnWithTypeAndName toVec(const std::vector<std::optional<String>> & v)
    {
        std::vector<String> strings;
        strings.reserve(v.size());
        for (std::optional<String> s : v)
        {
            strings.push_back(s.value());
        }
        return createColumn<String>(strings);
    }

    static ColumnWithTypeAndName toVec(const std::vector<std::optional<UInt64>> & v)
    {
        std::vector<UInt64> ints;
        ints.reserve(v.size());
        for (std::optional<UInt64> i : v)
        {
            ints.push_back(i.value());
        }
        return createColumn<UInt64>(ints);
    }

    static ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }

    static ColumnWithTypeAndName toConst(const UInt64 i)
    {
        return createConstColumn<UInt64>(1, i);
    }
};

TEST_F(StringCharLength, charLengthVector)
{
    std::vector<std::optional<String>> candidate_strings = {"", "a", "do you know my length?", "你知道我的长度吗？", "你知道我的 length 吗?？"};
    std::vector<std::optional<UInt64>> expect = {0, 1, 22, 9, 16};
    ASSERT_COLUMN_EQ(
        toNullableVec(expect),
        executeFunction(
            "lengthUTF8",
            toNullableVec(candidate_strings)));

    ASSERT_COLUMN_EQ(
        toVec(expect),
        executeFunction(
            "lengthUTF8",
            toVec(candidate_strings)));
}

TEST_F(StringCharLength, charLengthConst)
{
    ASSERT_COLUMN_EQ(
        toConst(0),
        executeFunction(
            "lengthUTF8",
            toConst("")));

    ASSERT_COLUMN_EQ(
        toConst(22),
        executeFunction(
            "lengthUTF8",
            toConst("do you know my length?")));

    ASSERT_COLUMN_EQ(
        toConst(9),
        executeFunction(
            "lengthUTF8",
            toConst("你知道我的长度吗？")));

    ASSERT_COLUMN_EQ(
        toConst(16),
        executeFunction(
            "lengthUTF8",
            toConst("你知道我的 length 吗?？")));
}

} // namespace tests
} // namespace DB
