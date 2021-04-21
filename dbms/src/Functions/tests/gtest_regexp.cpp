#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <Functions/FunctionsStringSearch.cpp>
#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{


class Regexp : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }
};

TEST_F(Regexp, TiDB_Match_Single_Test)
{
    UInt8 res = false;
    DB::MatchImpl<false, false, true>::constant_constant("a\nB\n", "(?m)(?i)^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "^b", '\\', "m", nullptr, res);
    ASSERT_TRUE(res == 1);
}
TEST_F(Regexp, TiDB_Match_Failed_Test)
{
    UInt8 res = false;
    DB::MatchImpl<false, false, true>::constant_constant("aa", "((((((((((a))))))))))\\10", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    /// back reference not supported in RE2
    // DB::MatchImpl<false, false, true>::constant_constant("abcabc", "(abc)\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constant_constant("abcabc", "([a-c]*)\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constant_constant("a", "(a)|\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constant_constant("x", "(a)|\\1", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // error ER_REGEXP_INVALID_BACK_REF
    // DB::MatchImpl<false,false,true>::constant_constant("-","\\1",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_INVALID_BACK_REF
    // DB::MatchImpl<false,false,true>::constant_constant("-","\\2",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_INVALID_BACK_REF
    // DB::MatchImpl<false,false,true>::constant_constant("-","(a)|\\2",'\\',"",nullptr,res); /* Result: c */;
    // DB::MatchImpl<false, false, true>::constant_constant("ababbbcbc", "(([a-c])b*?\\2)*", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constant_constant("ababbbcbc", "(([a-c])b*?\\2){3}", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constant_constant("aaxabxbaxbbx", "((\\3|b)\\2(a)x)+", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 0);
    // DB::MatchImpl<false, false, true>::constant_constant("aaaxabaxbaaxbbax", "((\\3|b)\\2(a)x)+", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    // DB::MatchImpl<false, false, true>::constant_constant("bbaababbabaaaaabbaaaabba", "((\\3|b)\\2(a)){2,}", '\\', "", nullptr, res);
    // ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AA", "(?i)((((((((((a))))))))))\\10", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABCABC", "(?i)(abc)\\1", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCABC", "(?i)([a-c]*)\\1", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abad", "a(?!b).", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abad", "a(?=d).", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abad", "a(?=c|d).", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaaaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "^(?:b|a(?=(.)))*\\1", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Ab4ab", "(?i)(ab)\\d\\1", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab4Ab", "(?i)(ab)\\d\\1", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaa", "^(a\\1?)(a\\1?)(a\\2?)(a\\3?)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaa", "^(a\\1?){4}$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Oo", "(?i)^(o)(?!.*\\1)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abc12bc", "(.*)\\d+\\1", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?<=a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("cb", "(?<=a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b", "(?<=a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?<!c)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("cb", "(?<!c)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b", "(?<!c)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaac", "^a(?#xxx){3}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaac", "(?x)^a (?#xxx) (?#yyy) {3}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("dbcb", "(?<![cd])b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("dbaacb", "(?<![cd])[ab]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("dbcb", "(?<!(c|d))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("dbaacb", "(?<!(c|d))[ab]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("cdaccb", "(?<!cd)[ab]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\nc\n", "((?s).)c(?!.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\nc\n", "((?s)b.)c(?!.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaab", "(?=(a+?))(\\1ab)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaab", "^(?=(a+?))\\1ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a", "$(?<=^(a))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaab", "(?>a+)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaab", "((?>a+)b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaab", "(?>(a+))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("((abc(ade)ufh()()x", "((?>[^()]+)|\\([^()]*\\))+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "(?m)\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "(?m)\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "a\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "a\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "a\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)a\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "(?m)a\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "(?m)a\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aa\nb\n", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa\n", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aa\nb\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)aa\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab\nb\n", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab\n", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab\nb\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abb\nb\n", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb\n", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abb\nb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)abb\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=c)c", '\\', "", nullptr, res); /* Result: yB */
    ;
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=b|c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=b|c)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=c|b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=c|b)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=[bc])", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?=[bc])c", '\\', "", nullptr, res); /* Result: yB */
    ;
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=b)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=b|c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=b|c)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=c|b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=c|b)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=[bc])", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)(?<=[bc])c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=c)c", '\\', "", nullptr, res); /* Result: yB */
    ;
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=b|c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=b|c)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=c|b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=c|b)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=[bc])", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?=[bc])c", '\\', "", nullptr, res); /* Result: yB */
    ;
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=b)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=b|c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=b|c)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=c|b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=c|b)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=[bc])", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)(?<=[bc])c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("2", "2(]*)?$\\1", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
}

TEST_F(Regexp, TiDB_Match_Test)
{
    UInt8 res = false;
    // Test based on extra/icu/tests/testdata/re_test.txt
    DB::MatchImpl<false, false, true>::constant_constant("abc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("xbc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("axc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abx", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("xabcy", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ababc", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbc", "ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", ".{1}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", ".{3,4}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab{0,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbc", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abq", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abq", "ab{1,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab{1,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab{1,3}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab{3,4}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab{4,5}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abbc", "ab?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab{0,1}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbbc", "ab?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab{0,1}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcc", "^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abcc", "^abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aabc", "^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("aabc", "abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aabcd", "abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "^", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("axc", "a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("axyzc", "a.*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("axyzd", "a.*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abd", "a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abd", "a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ace", "a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aac", "a[b-d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a-", "a[-b]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a-", "a[b-]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_INVALID_RANGE
    // DB::MatchImpl<false,false,true>::constant_constant("-","a[b-a]",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constant_constant("-","a[]b",'\\',"",nullptr,res); /* Result: ci */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constant_constant("-","a[",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("a]", "a]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a]b", "a[]]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aed", "a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abd", "a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("adc", "a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a-c", "a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a]c", "a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("adc", "a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a-", "\\ba\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("-a", "\\ba\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("-a-", "\\ba\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("xy", "\\by\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("yz", "\\by\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("xyz", "\\by\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a-", "\\Ba\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("-a", "\\Ba\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("-a-", "\\Ba\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("xy", "\\By\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("yz", "\\by\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("xyz", "\\By\\B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a", "\\w", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("-", "\\w", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a", "\\W", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("-", "\\W", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a b", "a\\sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a-b", "a\\sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a b", "a\\Sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a-b", "a\\Sb", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("1", "\\d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("-", "\\d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("1", "\\D", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("-", "\\D", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a", "[\\w]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("-", "[\\w]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a", "[\\W]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("-", "[\\W]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a b", "a[\\s]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a-b", "a[\\s]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a b", "a[\\S]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a-b", "a[\\S]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("1", "[\\d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("-", "[\\d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("1", "[\\D]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("-", "[\\D]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("def", "()ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("-","*a",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("-","(*)b",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("b", "$b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    // error ER_REGEXP_BAD_ESCAPE_SEQUENCE
    // DB::MatchImpl<false,false,true>::constant_constant("-","a\\",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("a(b", "a\\(b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a((b", "a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\\b", "a\\\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constant_constant("-","abc)",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constant_constant("-","(abc",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("abc", "((a))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "(a)b(c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aabbabc", "a+b+c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aabbabc", "a{1,}b{1,}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("-","a**",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("abcabc", "a.+?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(a+|b)*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(a+|b){0,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(a+|b)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(a+|b){1,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(a+|b)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(a+|b){0,1}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constant_constant("-",",'\\',"",nullptr,res);(",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("cde", "[^ab]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("", "abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("", "a*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abbbcd", "([abc])*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "([abc])*bcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("e", "a|b|c|d|e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ef", "(a|b|c|d|e)f", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcdefg", "abcd*efg", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("xabyabbbz", "ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("xayabbbz", "ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcde", "(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("hij", "[abhgefdc]ij", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcde", "^(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abcdef", "(abc|)ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(a|b)c*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "(ab|ab*)bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "a([bc]*)c*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "a([bc]*)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "a([bc]+)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "a([bc]*)(c+d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("adcdcde", "a[bcd]*dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("adcdcde", "a[bcd]+dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "(ab|a)b*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "((a)(b)c)(d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("alpha", "[a-zA-Z_][a-zA-Z0-9_]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abh", "^a(bc+|b[eh])g|.h$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("effgz", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ij", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("effg", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("bcdd", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("reffgz", "(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a", "((((((((((a))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a", "(((((((((a)))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("uh-uh", "multiple words of text", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("multiple words, yeah", "multiple words", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcde", "(.*)c(.*)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("(a, b)", "\\((.*), (.*)\\)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "[k]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "abcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "a(bc)d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ac", "a[-]?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b", "(a)|(b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("XBC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("AXC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABX", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("XABCY", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABABC", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBC", "(?i)ab*bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab*?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab{0,}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBC", "(?i)ab+?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABQ", "(?i)ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABQ", "(?i)ab{1,}bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab+bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab{1,}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab{1,3}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab{3,4}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab{4,5}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABBC", "(?i)ab??bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab??bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab{0,1}?bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBBC", "(?i)ab??bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab??c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab{0,1}?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCC", "(?i)^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABCC", "(?i)^abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AABC", "(?i)^abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("AABC", "(?i)abc$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)^", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AXC", "(?i)a.c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AXYZC", "(?i)a.*?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AXYZD", "(?i)a.*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABD", "(?i)a[bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABD", "(?i)a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ACE", "(?i)a[b-d]e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AAC", "(?i)a[b-d]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A-", "(?i)a[-b]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A-", "(?i)a[b-]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_INVALID_RANGE
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)a[b-a]",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)a[]b",'\\',"",nullptr,res); /* Result: ci */;
    // error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)a[",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("A]", "(?i)a]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A]B", "(?i)a[]]b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AED", "(?i)a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABD", "(?i)a[^bc]d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ADC", "(?i)a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A-C", "(?i)a[^-b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("A]C", "(?i)a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ADC", "(?i)a[^]b]c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)ab|cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("DEF", "(?i)()ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)*a",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)(*)b",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("B", "(?i)$b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    // error ER_REGEXP_BAD_ESCAPE_SEQUENCE
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)a\\",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("A(B", "(?i)a\\(b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A((B", "(?i)a\\(*b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A\\B", "(?i)a\\\\b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)abc)",'\\',"",nullptr,res); /* Result: c */;
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)(abc",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)((a))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)(a)b(c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AABBABC", "(?i)a+b+c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AABBABC", "(?i)a{1,}b{1,}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i)a**",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("ABCABC", "(?i)a.+?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCABC", "(?i)a.*?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCABC", "(?i)a.{0,5}?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(a+|b)*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(a+|b){0,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(a+|b)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(a+|b){1,}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(a+|b)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(a+|b){0,1}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(a+|b){0,1}?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?i))(",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("CDE", "(?i)[^ab]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("", "(?i)abc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("", "(?i)a*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABBBCD", "(?i)([abc])*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)([abc])*bcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("E", "(?i)a|b|c|d|e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("EF", "(?i)(a|b|c|d|e)f", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCDEFG", "(?i)abcd*efg", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("XABYABBBZ", "(?i)ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("XAYABBBZ", "(?i)ab*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCDE", "(?i)(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("HIJ", "(?i)[abhgefdc]ij", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCDE", "(?i)^(ab|cd)e", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABCDEF", "(?i)(abc|)ef", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)(a|b)c*d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)(ab|ab*)bc", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)a([bc]*)c*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)a([bc]*)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)a([bc]+)(c*d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)a([bc]*)(c+d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ADCDCDE", "(?i)a[bcd]*dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ADCDCDE", "(?i)a[bcd]+dcdcde", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABC", "(?i)(ab|a)b*c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)((a)(b)c)(d)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ALPHA", "(?i)[a-zA-Z_][a-zA-Z0-9_]*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABH", "(?i)^a(bc+|b[eh])g|.h$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("EFFGZ", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("IJ", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("EFFG", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("BCDD", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("REFFGZ", "(?i)(bc+d$|ef*g.|h?i(j|k))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A", "(?i)((((((((((a))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A", "(?i)(((((((((a)))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("A", "(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("C", "(?i)(?:(?:(?:(?:(?:(?:(?:(?:(?:(a|b|c))))))))))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("UH-UH", "(?i)multiple words of text", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("MULTIPLE WORDS, YEAH", "(?i)multiple words", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCDE", "(?i)(.*)c(.*)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("(A, B)", "(?i)\\((.*), (.*)\\)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)[k]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)abcd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ABCD", "(?i)a(bc)d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AC", "(?i)a[-]?c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ace", "a(?:b|c|d)(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ace", "a(?:b|c|d)*(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ace", "a(?:b|c|d)+?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d)+?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d)+(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){2}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){4,5}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){4,5}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("foobar", "((foo)|(bar))*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // error ER_REGEXP_MISMATCHED_PAREN
    // DB::MatchImpl<false,false,true>::constant_constant("-",":(?:",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){6,7}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){6,7}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){5,6}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){5,6}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){5,7}(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("acdbcdbe", "a(?:b|c|d){5,7}?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ace", "a(?:b|(c|e){1,2}?|d)+?(.)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "^(.+)?B", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant(".", "^([^a-z])|(\\^)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("<&OUT", "^[<>]&", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("aaaaaaaaaa","^(a(?(1)\\1)){4}$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("aaaaaaaaa","^(a(?(1)\\1)){4}$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("aaaaaaaaaaa","^(a(?(1)\\1)){4}$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaaaaa", "((a{4})+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaaaaaa", "(((aa){2})+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaaaaaaaaa", "(((a{2}){2})+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("foobar", "(?:(f)(o)(o)|(b)(a)(r))*", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?<%)b",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("aba", "(?:..)*a", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aba", "(?:..)*?a", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "^(){3,5}", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aax", "^(a+)*ax", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aax", "^((a|b)+)*ax", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aax", "^((a|bc)+)*ax", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("cab", "(a|x)*ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("cab", "(a)*ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?:(?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "((?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "(?:(?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "((?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "(?:(?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "((?i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "((?i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "(?i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "((?i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "(?i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "((?i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(?:(?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)((?-i)a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aB", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("Ab", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)(?-i:a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("AB", "(?i)((?-i:a))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a\nB", "(?i)((?-i:a.))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a\nB", "(?i)((?s-i:a.))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("B\nB", "(?i)((?s-i:a.))b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant(
        "cabbbb", "(?:c|d)(?:)(?:a(?:)(?:b)(?:b(?:))(?:b(?:)(?:b)))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("caaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "(?:c|d)(?:)(?:aaaaaaaa(?:)(?:bbbbbbbb)(?:bbbbbbbb(?:))(?:bbbbbbbb(?:)(?:bbbbbbbb)))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("foobar1234baz", "foo\\w*\\d{4}baz", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("cabd","a(?{})b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("-","a(?{)b",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("-","a(?{{})b",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("-","a(?{}})b",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("-","a(?{"{"})b",'\\',"",nullptr,res); /* Result: c */;
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("cabd","a(?{"\\{"})b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("-","a(?{"{"}})b",'\\',"",nullptr,res); /* Result: c */;
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("caxbd","a(?{$bl="\\{"}).b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("x~~", "x(~~)*(?:(?:F)?)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a--", "^(?:a?b?)*$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\nc\n", "((?s)^a(.))((?m)^b$)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\nc\n", "((?m)^b$)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)^(b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "((?m)^b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "\n((?m)^b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\nc\n", "^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\nc\n", "()^b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\nc\n", "((?m)^b)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(1)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(x)?(?(1)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(x)?(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","()?(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","()(?(1)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","()?(?(1)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("(blah)","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("blah","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("blah)","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("(blah","^(\\()?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("(blah)","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("blah","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("blah)","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("(blah","^(\\(+)?blah(?(1)(\\)))$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(1?)a|b)",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(1)a|b|c)",'\\',"",nullptr,res); /* Result: c */;
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?{0})a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?{0})b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?{1})b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?{1})a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?!a)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?!a)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?=a)b|a)",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","(?(?=a)a|b)",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("one:", "(\\w+:)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd:", "([\\w:]+::)?(\\w+)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "([\\w:]+::)?(\\w+)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("xy:z:::abcd", "([\\w:]+::)?(\\w+)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aexycd", "^[^bcd]*(c+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("caab", "(a*)b+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("yaaxxaaaacd","(?{$a=2})a*aa(?{local$a=$a+1})k*c(?{$b=$a})",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("yaaxxaaaacd","(?{$a=2})(a(?{local$a=$a+1}))*aak*c(?{$b=$a})",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaab", "(>a+)ab", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a:[b]:", "([\\[:]+)", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a=[b]=", "([\\[=]+)", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a.[b].", "([\\[.]+)", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_MISSING_CLOSE_BRACKET
    // DB::MatchImpl<false,false,true>::constant_constant("-","[a[:xyz:",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constant_constant("-","[a[:xyz:]",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("abc", "[a\\[:]b[:c]", '\\', "", nullptr, res); /* Result: yi */
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constant_constant("pbaq","([a[:xyz:]b]+)",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("abc", "[a\\[:]b[:c]", '\\', "", nullptr, res); /* Result: iy */
    ASSERT_TRUE(res == 1);
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constant_constant("-","[[:foo:]]",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_ILLEGAL_ARGUMENT
    // DB::MatchImpl<false,false,true>::constant_constant("-","[[:^foo:]]",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_LOOK_BEHIND_LIMIT
    // DB::MatchImpl<false,false,true>::constant_constant("-","(?<=x+)y",'\\',"",nullptr,res); /* Result: c */;
    // --error ER_REGEXP_MAX_LT_MIN
    // DB::MatchImpl<false,false,true>::constant_constant("-","a{37,17}",'\\',"",nullptr,res); /* Result: c */;
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "(?m)\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "(?m)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "(?m)\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "(?m)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    /// different from mysql
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "(?m)a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\na\n", "(?m)a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "(?m)a\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\na", "(?m)a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aa\nb\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("aa\nb\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    /// different from mysql
    DB::MatchImpl<false, false, true>::constant_constant("b\naa\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aa\nb\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("aa\nb\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\naa", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)aa\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab\nb\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab\nb\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    /// different from mysql
    DB::MatchImpl<false, false, true>::constant_constant("b\nab\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab\nb\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ab\nb\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nab", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "ab\\Z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)ab\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)ab$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abb\nb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abb\nb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abb\nb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abb\nb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("b\nabb", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ac\nb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nac", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca\nb\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca\n", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)abb\\z", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("b\nca", "(?m)abb$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("ca", "(^|x)(c)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("x", "a*abc?xyz+pqr{3}ab{2,}xy{4,5}pq{0,6}AB{0,}zz", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("yabz","a(?{$a=2;$b=3;($b)=$a})b",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("_I(round(xs * sz),1)", "round\\(((?>[^()]+))\\)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("x ", "(?x)((?x:.) )", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("x ", "(?x)((?-x:.) )", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("foo.bart", "foo.bart", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd\ndxxx", "(?m)^d[x][x][x]", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("xxxtt", "tt+$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("za-9z", "([a\\-\\d]+)", '\\', "", nullptr, res); /* Result: yi */
    ;
    DB::MatchImpl<false, false, true>::constant_constant("a0-za", "([\\d-z]+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a0- z", "([\\d-\\s]+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("za-9z", "([a-[:digit:]]+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("=0-z=", "([[:digit:]-z]+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("=0-z=", "([[:digit:]-[:alpha:]]+)", '\\', "", nullptr, res); /* Result: iy */
    ;
    DB::MatchImpl<false, false, true>::constant_constant("aaaXbX", "\\GX.*X", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("3.1415926", "(\\d+\\.\\d+)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("have a web browser", "(\\ba.{0,10}br)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("Changes", "(?i)\\.c(pp|xx|c)?$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("IO.c", "(?i)\\.c(pp|xx|c)?$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("IO.c", "(?i)(\\.c(pp|xx|c)?$)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("C:/", "^([a-z]:)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("\nx aa", "(?m)^\\S\\s+aa$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(^|a)b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abac", "^([ab]*?)(b)?(c)$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcab", "(\\w)?(abc)\\1b", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a,b,c", "^(?:.,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a,b,c", "^(.,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a,b,c", "^(?:[^,]*,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a,b,c", "^([^,]*,){2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]*,){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]*,){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]*,){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{1,3},){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{1,3},){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{1,3},){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{1,},){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{1,},){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{1,},){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{0,3},){3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{0,3},){3,}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaa,b,c,d", "^([^,]{0,3},){0,3}d", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("", "(?i)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nxb\n", "(?m)(?!\\A)x", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aba", "^(a(b)?)+$", '\\', "", nullptr, res); /* Result: yi */
    ;
    DB::MatchImpl<false, false, true>::constant_constant("123\nabcabcabcabc\n", "(?m)^.{9}abc.*\n", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a", "^(a)?a$", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("a","^(a)?(?(1)a|b)+$",'\\',"",nullptr,res); ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("x1", "^(0+)?(?:x(1))?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant(
        "012cxx0190", "^([0-9a-fA-F]+)(?:x([0-9a-fA-F]+)?)(?:x([0-9a-fA-F]+))?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("bbbac", "^(b+?|a){1,2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("bbbbac", "^(b+?|a){1,2}c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("cd. (A. Tw)", "\\((\\w\\. \\w+)\\)", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("aaaacccc", "((?:aaaa|bbbb)cccc)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("bbbbcccc", "((?:aaaa|bbbb)cccc)?", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a", "(a)?(a)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("ab", "(ab)?(ab)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abc", "(abc)?(abc)+", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("a\nb\n", "(?m)b\\s^", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("a", "\\ba", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # ?? Not supported
    // --error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("ab","^(a(??{"(?!)"})|(a)(?{1}))b",'\\',"",nullptr,res); /* Result: yi */;
    DB::MatchImpl<false, false, true>::constant_constant("AbCd", "ab(?i)cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 0);
    DB::MatchImpl<false, false, true>::constant_constant("abCd", "ab(?i)cd", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("CD","(A|B)*(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("ABCD","(A|B)*(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("CD","(A|B)*?(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    // # Not implemented
    // --error ER_REGEXP_UNIMPLEMENTED
    // DB::MatchImpl<false,false,true>::constant_constant("ABCD","(A|B)*?(?(1)(CD)|(CD))",'\\',"",nullptr,res); ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("foo\n bar", "(?m:(foo\\s*$))", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    DB::MatchImpl<false, false, true>::constant_constant("abcd", "(.*?)c", '\\', "", nullptr, res);
    ASSERT_TRUE(res == 1);
    // # ?? not supported
    // --error ER_REGEXP_RULE_SYNTAX
    // DB::MatchImpl<false,false,true>::constant_constant("x","(??{})",'\\',"",nullptr,res); /* Result: yi */;
}


TEST_F(Regexp, regexp_replace_one_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();
    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field("  hello   ", 10));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    Block testBlock;
    auto type = std::make_shared<DataTypeString>();

    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, type, "test_trim_const");

    ColumnsWithTypeAndName ctns{ctn};
    testBlock.insert(ctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert({});
    testBlock.insert({});
    testBlock.insert({});
    ColumnNumbers cns{0};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 1);

    const IColumn * res = testBlock.getByPosition(1).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);


    Field resField;

    std::vector<String> results{"hello", "hello", "hello", "hello", "hello"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"hello   ", "hello   ", "hello   ", "hello   ", "hello   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {
        "  hello",
        "  hello",
        "  hello",
        "  hello",
        "  hello",
    };
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

TEST_F(Regexp, regexp_replace_all_Test)
{
    const Context context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();
    MutableColumnPtr cp = ColumnString::create();
    cp->insert(Field("  hello   ", 10));
    MutableColumnPtr excp = ColumnString::create();
    excp->insert(Field(" hoe", 10));

    ColumnPtr csp = ColumnConst::create(cp->getPtr(), 5);
    ColumnPtr excsp = ColumnConst::create(excp->getPtr(), 5);
    Block testBlock;

    ColumnWithTypeAndName ctn = ColumnWithTypeAndName(csp, std::make_shared<DataTypeString>(), "test_trim_const");
    ColumnWithTypeAndName exctn = ColumnWithTypeAndName(excsp, std::make_shared<DataTypeString>(), "test_ex_trim_const");

    ColumnsWithTypeAndName ctns{ctn, exctn};
    testBlock.insert(ctn);
    testBlock.insert(exctn);
    // for result from trim, ltrim and rtrim
    testBlock.insert({});
    testBlock.insert({});
    testBlock.insert({});
    ColumnNumbers cns{0, 1};

    // test trim
    auto bp = factory.tryGet("trim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);

    const IColumn * res = testBlock.getByPosition(2).column.get();
    const ColumnString * c0_string = checkAndGetColumn<ColumnString>(res);


    Field resField;

    std::vector<String> results{"ll", "ll", "ll", "ll", "ll"};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test ltrim
    bp = factory.tryGet("ltrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 2);
    res = testBlock.getByPosition(2).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {"llo   ", "llo   ", "llo   ", "llo   ", "llo   "};
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }

    // test rtrim
    bp = factory.tryGet("rtrim", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_TRUE(bp->isVariadic());

    bp->build(ctns)->execute(testBlock, cns, 3);
    res = testBlock.getByPosition(3).column.get();
    c0_string = checkAndGetColumn<ColumnString>(res);

    results = {
        "  hell",
        "  hell",
        "  hell",
        "  hell",
        "  hell",
    };
    for (size_t t = 0; t < results.size(); t++)
    {
        c0_string->get(t, resField);
        String s = resField.get<String>();
        EXPECT_EQ(results[t], s);
    }
}

} // namespace tests
} // namespace DB
