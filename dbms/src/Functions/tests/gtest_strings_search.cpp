#include <Functions/registerFunctions.h>
#include <Functions/FunctionsStringSearch.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Functions/FunctionFactory.h>

namespace DB {
namespace tests {
class StringMatch : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        try {
            registerFunctions();
        }
        catch (DB::Exception &) {
            // Just Ignore the exception, maybe another test has already registered.
        }
    }
};

TEST_F(StringMatch, Like3ArgsVectorWithVector) {
    /**
     * With LIKE you can use the following two wildcard characters in the pattern:
     * * % matches any number of characters, even zero characters.
     * * _ matches exactly one character.
     */
    struct Case {
        int match;
        std::string a;
        std::string b;
    };
    std::vector<Case> cases = {
            {1, "",               ""},
            {1, "a",              "a"},
            {1, "",               ""},
            {1, "a",              "%"},
            {1, "a",              "a%"},
            {1, "a",              "%a"},
            {1, "ab",             "a%"},
            {1, "ab",             "ab"},
            // pattern can only be used as the second argument
            {0, "a%",             "ab"},
            {1, "aaaa",           "a%"},
            {0, "aaaa",           "aaab%"},
            {1, "aabaababaabbab", "aab%a%aab%b"},
            {1, "a",              "_"},
            {1, "abab",           "_b__"},
            {0, "abab",           "_b_"},
    };

    MutableColumnPtr csp0(ColumnString::create());
    MutableColumnPtr csp1(ColumnString::create());

    auto cc2 = ColumnInt32::create();
    ColumnInt32::Container &vec = cc2->getData();
    vec.resize(1);
    vec[0] = Int32('\\');
    ColumnPtr ccp2(ColumnConst::create(cc2->getPtr(), 1));
    std::vector<int> results;

    for (auto &&cas: cases) {
        csp0->insert(Field(cas.a.c_str(), cas.a.size()));
        csp1->insert(Field(cas.b.c_str(), cas.b.size()));
        results.push_back(cas.match);
    }

    Block test_block;
    auto ctn0 = ColumnWithTypeAndName(std::move(csp0), std::make_shared<DataTypeString>(), "test_like_0");
    auto ctn1 = ColumnWithTypeAndName(std::move(csp1), std::make_shared<DataTypeString>(), "test_like_1");
    auto ctn2 = ColumnWithTypeAndName(std::move(ccp2), std::make_shared<DataTypeInt32>(), "test_like_2");
    ColumnsWithTypeAndName ctns{ctn0, ctn1, ctn2};
    test_block.insert(ctn0);
    test_block.insert(ctn1);
    test_block.insert(ctn2);
    // for results
    test_block.insert({});
    ColumnNumbers cns{0, 1, 2};

    Context context = TiFlashTestEnv::getContext();

    auto &factory = FunctionFactory::instance();

    auto like_func = factory.tryGet("like3Args", context);
    ASSERT_TRUE(like_func != nullptr);
    ASSERT_FALSE(like_func->isVariadic());

    like_func->build(ctns)->execute(test_block, cns, 3);
    const IColumn *res = test_block.getByPosition(3).column.get();
    const ColumnUInt8 *res_string = checkAndGetColumn<ColumnUInt8>(res);

    Field res_field;
    for (size_t t = 0; t < results.size(); t++) {
        res_string->get(t, res_field);
        Int64 res_val = res_field.get<UInt8>();
        EXPECT_EQ(results[t], res_val);
    }
}

TEST_F(StringMatch, Like3ArgsConstantWithVector)
try
{
    /**
     * With LIKE you can use the following two wildcard characters in the pattern:
     * * % matches any number of characters, even zero characters.
     * * _ matches exactly one character.
     */

    struct Case {
        std::string src;
        std::vector<std::pair<std::string, int>> pat;
    };
    std::vector<Case> cases = {
            {"a",   {{"b",   0}, {"a",   1}, {"_",   1}, {"%",   1}}},
            {"aab", {{"aab", 1}, {"ab_", 0}, {"a_a", 0}, {"a__", 1}}},
    };

    for (auto &&cas: cases) {
        MutableColumnPtr cc0 = ColumnString::create();
        cc0->insert(Field(cas.src.c_str(), cas.src.size()));
        ColumnPtr ccp0 = ColumnConst::create(cc0->getPtr(), 1);

        MutableColumnPtr csp1 = ColumnString::create();
        std::vector<int> results;

        for (auto &&pat: cas.pat) {
            csp1->insert(Field(pat.first.c_str(), pat.first.size()));
            results.push_back(pat.second);
        }

        auto cc2 = ColumnInt32::create();
        ColumnInt32::Container &vec = cc2->getData();
        vec.resize(1);
        vec[0] = Int32('\\');
        ColumnPtr ccp2(ColumnConst::create(cc2->getPtr(), 1));


        Block test_block;
        auto ctn0 = ColumnWithTypeAndName(std::move(ccp0), std::make_shared<DataTypeString>(), "test_like_0");
        auto ctn1 = ColumnWithTypeAndName(std::move(csp1), std::make_shared<DataTypeString>(), "test_like_1");
        auto ctn2 = ColumnWithTypeAndName(std::move(ccp2), std::make_shared<DataTypeInt32>(), "test_like_2");
        ColumnsWithTypeAndName ctns{ctn0, ctn1, ctn2};
        test_block.insert(ctn0);
        test_block.insert(ctn1);
        test_block.insert(ctn2);
        // for results
        test_block.insert({});
        ColumnNumbers cns{0, 1, 2};

        Context context = TiFlashTestEnv::getContext();

        auto &factory = FunctionFactory::instance();

        auto like_func = factory.tryGet("like3Args", context);
        ASSERT_TRUE(like_func != nullptr);
        ASSERT_FALSE(like_func->isVariadic());

        auto func = like_func->build(ctns);
        func->execute(test_block, cns, 3);

        const IColumn *res = test_block.getByPosition(3).column.get();
        const ColumnUInt8 *res_string = checkAndGetColumn<ColumnUInt8>(res);

        Field res_field;
        for (size_t t = 0; t < results.size(); t++) {
            res_string->get(t, res_field);
            Int64 res_val = res_field.get<UInt8>();
            EXPECT_EQ(results[t], res_val);
        }
    }
} CATCH
}

}
