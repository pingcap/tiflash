#include <Functions/registerFunctions.h>
#include <Functions/FunctionsStringSearch.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace tests
{
class StringMatch : public ::testing::Test
{
protected:
    Context context;
    void SetUp() override
    {
        context = TiFlashTestEnv::getContext();
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Just Ignore the exception, maybe another test has already registered.
        }
    }
};

TEST_F(StringMatch, Like3ArgsVectorWithVector)
{
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
            { 1, "", ""},
            {1, "a", "a"},
            {1, "", ""},
            {1, "a", "%"},
            {1, "a", "a%"},
            {1, "a", "%a"},
            {1, "ab", "a%"},
            {1, "ab", "ab"},
            // pattern can only be used as the second argument
            {0, "a%", "ab"},
            {1, "aaaa", "a%"},
            {0, "aaaa", "aaab%"},
    };

    MutableColumnPtr csp0(ColumnString::create());
    MutableColumnPtr csp1(ColumnString::create());

    auto cc2 = ColumnInt32::create();
    ColumnInt32::Container & vec = cc2->getData();
    vec.resize(1);
    vec[0] = Int32('\\');
    ColumnPtr ccp2(ColumnConst::create(cc2->getPtr(), 1));
    std::vector<int> results;

    for (auto &&cas : cases) {
        csp0->insert(Field(cas.a.c_str(), cas.a.size()));
        csp1->insert(Field(cas.b.c_str(), cas.b.size()));
        results.push_back(cas.match);
    }

    Block test_block;
    auto ctn0 = ColumnWithTypeAndName(std::move(csp0),  std::make_shared<DataTypeString>(), "test_like_0");
    auto ctn1 = ColumnWithTypeAndName(std::move(csp1),  std::make_shared<DataTypeString>(), "test_like_1");
    auto ctn2 = ColumnWithTypeAndName(std::move(ccp2),  std::make_shared<DataTypeInt32>(), "test_like_2");
    ColumnsWithTypeAndName ctns{ctn0, ctn1};
    test_block.insert(ctn0);
    test_block.insert(ctn1);
    test_block.insert(ctn2);
    // for results
    test_block.insert({});
    ColumnNumbers cns{0, 1, 2};

    auto & factory = FunctionFactory::instance();

    auto like_func = factory.tryGet(DB::NameLike3Args::name, context);
    ASSERT_TRUE(like_func != nullptr);
    ASSERT_FALSE(like_func->isVariadic());

    like_func->build(ctns)->execute(test_block, cns, 3);
    const IColumn * res = test_block.getByPosition(3).column.get();
    const ColumnUInt8 * res_string = checkAndGetColumn<ColumnUInt8>(res);

    Field res_field;
    for (size_t t = 0; t < results.size(); t++)
    {
        res_string->get(t, res_field);
        Int64 res_val = res_field.get<UInt8>();
        EXPECT_EQ(results[t], res_val);
    }
}
}
}
