#include <Core/BlockGen.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ctime>
#include <ext/scope_guard.h>
#include <memory>

namespace DB
{
namespace DM
{
namespace tests
{

static const ColId  DEFAULT_COL_ID   = 0;
static const String DEFAULT_COL_NAME = "2020-09-26";

class DMMinMaxIndex_test : public ::testing::Test
{
public:
    DMMinMaxIndex_test() {}

protected:
    static void SetUpTestCase() {}

    void SetUp() override { context = std::make_unique<Context>(DMTestEnv::getContext()); }

private:
protected:
    // a ptr to context, we can reload context with different settings if need.
    std::unique_ptr<Context> context;
};

Attr attr(String type)
{
    return Attr{DEFAULT_COL_NAME, DEFAULT_COL_ID, DataTypeFactory::instance().get(type)};
}

bool checkMatch(const String &        test_case, //
                Context &             context,
                const String &        type,
                const CSVTuples       block_tuples,
                const RSOperatorPtr & filter)
{
    String name = "DMMinMaxIndex_test#" + test_case;
    String path = DB::tests::TiFlashTestEnv::getTemporaryPath() + name;

    auto clean_up = [&]() {
        const String p = DB::tests::TiFlashTestEnv::getTemporaryPath();
        if (Poco::File f{p}; f.exists())
        {
            f.remove(true);
            f.createDirectories();
        }
    };

    clean_up();
    SCOPE_EXIT({ clean_up(); });

    RowKeyRange all_range = RowKeyRange::newAll(false, 1);

    ColumnDefine cd(DEFAULT_COL_ID, DEFAULT_COL_NAME, DataTypeFactory::instance().get(type));

    ColumnDefines table_columns;
    table_columns.push_back(getExtraHandleColumnDefine(false));
    table_columns.push_back(getVersionColumnDefine());
    table_columns.push_back(getTagColumnDefine());
    table_columns.push_back(cd);

    Block header = toEmptyBlock(table_columns);
    Block block  = genBlock(header, block_tuples);

    DeltaMergeStorePtr store = std::make_shared<DeltaMergeStore>(
        context, false, "test_database", "test_table", table_columns, getExtraHandleColumnDefine(false), false, 1);

    store->write(context, context.getSettingsRef(), std::move(block));
    store->flushCache(context, all_range);
    store->mergeDeltaAll(context);

    auto streams = store->read(context, context.getSettingsRef(), {cd}, {all_range}, 1, MAX_UINT64, filter);
    streams[0]->readPrefix();
    auto rows = streams[0]->read().rows();
    streams[0]->readSuffix();

    return rows != 0;
}

bool checkMatch(const String & test_case, Context & context, String type, String value, const RSOperatorPtr & filter)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{"0", "0", "0", value}};
    return checkMatch(test_case, context, type, tuples, filter);
}

TEST_F(DMMinMaxIndex_test, Basic)
try
{
    // clang-format off
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Int64", "100", createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", "100", createEqual(attr("Int64"), Field((Int64)101))));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Int64", "100", createIn(attr("Int64"), {Field((Int64)100)})));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", "100", createIn(attr("Int64"), {Field((Int64)101)})));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Int64", "100", createGreater(attr("Int64"), Field((Int64)99), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", "100", createGreater(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Int64", "100", createGreaterEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", "100", createGreaterEqual(attr("Int64"), Field((Int64)101), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Int64", "100", createLess(attr("Int64"), Field((Int64)101), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", "100", createLess(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Int64", "100", createLessEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", "100", createLessEqual(attr("Int64"), Field((Int64)99), 0)));

    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createEqual(attr("Date"), Field((String) "2020-09-27"))));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createEqual(attr("Date"), Field((String) "2020-09-28"))));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createIn(attr("Date"), {Field((String) "2020-09-27")})));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createIn(attr("Date"), {Field((String) "2020-09-28")})));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createGreater(attr("Date"), Field((String) "2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createGreater(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createGreaterEqual(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createGreaterEqual(attr("Date"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createLess(attr("Date"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createLess(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createLessEqual(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Date", "2020-09-27", createLessEqual(attr("Date"), Field((String) "2020-09-26"), 0)));

    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"))));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"))));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-27")})));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-28")})));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "MyDateTime", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));

    /// Currently we don't do filtering for null values. i.e. if a pack contains any null values, then the pack will pass the filter.
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createEqual(attr("Nullable(Int64)"), Field((Int64)101))));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createIn(attr("Nullable(Int64)"), {Field((Int64)101)})));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createGreater(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createGreaterEqual(attr("Nullable(Int64)"), Field((Int64)101), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createLess(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createLessEqual(attr("Nullable(Int64)"), Field((Int64)99), 0)));
    // clang-format on
}
CATCH

TEST_F(DMMinMaxIndex_test, Logical)
try
{
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", "100", createNot(createEqual(attr("Int64"), Field((Int64)100)))));
    ASSERT_EQ(false,
              checkMatch(__FUNCTION__,
                         *context,
                         "Int64",
                         "100",
                         createAnd({createEqual(attr("Int64"), Field((Int64)101)), createEqual(attr("Int64"), Field((Int64)100))})));
    ASSERT_EQ(true,
              checkMatch(__FUNCTION__,
                         *context,
                         "Int64",
                         "100",
                         createOr({createEqual(attr("Int64"), Field((Int64)101)), createEqual(attr("Int64"), Field((Int64)100))})));
}
CATCH

TEST_F(DMMinMaxIndex_test, DelMark)
try
{
    ASSERT_EQ(true, checkMatch(__FUNCTION__, *context, "Int64", {{"0", "0", "0", "100"}}, createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(false, checkMatch(__FUNCTION__, *context, "Int64", {{"0", "0", "1", "100"}}, createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(true,
              checkMatch(__FUNCTION__,
                         *context,
                         "Int64",
                         {{"0", "0", "1", "100"}, {"1", "1", "0", "100"}},
                         createGreaterEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false,
              checkMatch(__FUNCTION__,
                         *context,
                         "Int64",
                         {{"0", "0", "1", "88"}, {"1", "1", "0", "100"}},
                         createLess(attr("Int64"), Field((Int64)100), 0)));
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
