// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Logger.h>
#include <Core/BlockGen.h>
#include <DataTypes/DataTypeEnum.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>
#include <Storages/DeltaMerge/Index/ValueComparison.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ext/scope_guard.h>
#include <memory>

namespace DB::DM::tests
{

class MinMaxIndexTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        context = DMTestEnv::getContext();
        if (!context->getMinMaxIndexCache())
        {
            context->setMinMaxIndexCache(DEFAULT_MARK_CACHE_SIZE);
        }
    }

    void TearDown() override { context->dropMinMaxIndexCache(); }

protected:
    // a ptr to context, we can reload context with different settings if need.
    ContextPtr context;
};

namespace
{
static constexpr ColId DEFAULT_COL_ID = 0;
static const String DEFAULT_COL_NAME = "2020-09-26";

Attr attr(String type)
{
    return Attr{DEFAULT_COL_NAME, DEFAULT_COL_ID, DataTypeFactory::instance().get(type)};
}

Attr pkAttr()
{
    const ColumnDefine & col = getExtraHandleColumnDefine(true);
    return Attr{col.name, col.id, col.type};
}

// Check if the data in `block_tuples` match `filter`.
bool checkMatch(
    const String & test_case,
    Context & context,
    const String & type,
    const CSVTuples block_tuples,
    const RSOperatorPtr & filter,
    bool is_common_handle = false,
    bool check_pk = false)
{
    String name = "MinMaxIndexTest_" + test_case;
    // We cannot restore tables with the same table id multiple times in a single run.
    // Because we don't update max_page_id for PS instance at run time.
    // And when restoring table, it will use the max_page_id from PS as the start point for allocating page id.
    // So if we restore the same table multiple times in a single run, it may write different data using the same page id.
    static int next_table_id = 100;

    auto clean_up = [&]() {
        context.dropMinMaxIndexCache();
    };

    clean_up();
    SCOPE_EXIT({ clean_up(); });

    RowKeyRange all_range = RowKeyRange::newAll(is_common_handle, 1);

    ColumnDefine cd(DEFAULT_COL_ID, DEFAULT_COL_NAME, DataTypeFactory::instance().get(type));

    ColumnDefines table_columns;
    table_columns.push_back(getExtraHandleColumnDefine(is_common_handle));
    table_columns.push_back(getVersionColumnDefine());
    table_columns.push_back(getTagColumnDefine());
    table_columns.push_back(cd);

    Block header = toEmptyBlock(table_columns);
    Block block = genBlock(header, block_tuples);

    // max page id is only updated at restart, so we need recreate page v3 before recreate table
    DeltaMergeStorePtr store = DeltaMergeStore::create(
        context,
        false,
        "test_database",
        name,
        NullspaceID,
        /*table_id*/ next_table_id++,
        true,
        table_columns,
        getExtraHandleColumnDefine(is_common_handle),
        is_common_handle,
        1);

    store->write(context, context.getSettingsRef(), block);
    store->flushCache(context, all_range);
    store->mergeDeltaAll(context);

    const ColumnDefine & col_to_read = check_pk ? getExtraHandleColumnDefine(is_common_handle) : cd;
    auto streams = store->read(
        context,
        context.getSettingsRef(),
        {col_to_read},
        {all_range},
        1,
        std::numeric_limits<UInt64>::max(),
        std::make_shared<PushDownFilter>(filter),
        std::vector<RuntimeFilterPtr>{},
        0,
        name,
        false);
    auto rows = getInputStreamNRows(streams[0]);
    store->drop();

    return rows != 0;
}

bool checkDelMatch(
    const String & test_case,
    Context & context,
    const String & type,
    const String & value,
    const RSOperatorPtr & filter)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{"0", "0", "1", value}};
    return checkMatch(test_case, context, type, tuples, filter);
}

bool checkPkMatch(
    const String & test_case,
    Context & context,
    const String & type,
    const String & pk_value,
    const RSOperatorPtr & filter,
    bool is_common_handle)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{pk_value, "0", "0", "0"}};
    return checkMatch(test_case, context, type, tuples, filter, is_common_handle, true);
}

enum MinMaxTestDatatype
{
    Test_Int64 = 0,
    Test_Nullable_Int64,
    Test_Date,
    Test_Nullable_Date,
    Test_DateTime,
    Test_Nullable_DateTime,
    Test_MyDateTime,
    Test_Nullable_MyDateTime,
    Test_Decimal64,
    Test_Nullable_Decimal64,
    Test_Max,
};

bool isNullableDateType(MinMaxTestDatatype data_type)
{
    switch (data_type)
    {
    case Test_Nullable_Int64:
    case Test_Nullable_Date:
    case Test_Nullable_DateTime:
    case Test_Nullable_MyDateTime:
    case Test_Nullable_Decimal64:
        return true;
    default:
        return false;
    }
}

enum MinMaxTestOperator
{
    Test_Equal = 0,
    Test_In,
    Test_Greater,
    Test_GreaterEqual,
    Test_Less,
    Test_LessEqual,
    Test_MaxOperator,
    Test_IsNull,
};

Decimal64 getDecimal64(String s)
{
    Decimal64 expected_default_value;
    ReadBufferFromString buf(s);
    readDecimalText(expected_default_value, buf, /*precision*/ 20, /*scale*/ 5);
    return expected_default_value;
}

static constexpr Int64 Int64_Match_DATA = 100;
static constexpr Int64 Int64_Greater_DATA = 10000;
static constexpr Int64 Int64_Smaller_DATA = -1;

static const String Date_Match_DATA = "2020-09-27";
static const String Date_Greater_DATA = "2022-09-27";
static const String Date_Smaller_DATA = "1997-09-27";

static const String DateTime_Match_DATA = "2020-01-01 05:00:01";
static const String DateTime_Greater_DATA = "2022-01-01 05:00:01";
static const String DateTime_Smaller_DATA = "1997-01-01 05:00:01";

static const String MyDateTime_Match_DATE = "2020-09-27";
static const String MyDateTime_Greater_DATE = "2022-09-27";
static const String MyDateTime_Smaller_DATE = "1997-09-27";

static const String Decimal_Match_DATA = "100.25566";
static const String Decimal_UnMatch_DATA = "100.25500";

std::pair<String, CSVTuples> generateTypeValue(MinMaxTestDatatype data_type, bool has_null)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        return {"Int64", {{"0", "0", "0", DB::toString(Int64_Match_DATA)}}};
    }
    case Test_Nullable_Int64:
    {
        if (has_null)
        {
            return {"Nullable(Int64)", {{"0", "0", "0", DB::toString(Int64_Match_DATA)}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(Int64)", {{"0", "0", "0", DB::toString(Int64_Match_DATA)}}};
    }
    case Test_Date:
    {
        return {"Date", {{"0", "0", "0", Date_Match_DATA}}};
    }
    case Test_Nullable_Date:
    {
        if (has_null)
        {
            return {"Nullable(Date)", {{"0", "0", "0", Date_Match_DATA}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(Date)", {{"0", "0", "0", Date_Match_DATA}}};
    }
    case Test_DateTime:
    {
        return {"DateTime", {{"0", "0", "0", DateTime_Match_DATA}}};
    }
    case Test_Nullable_DateTime:
    {
        if (has_null)
        {
            return {"Nullable(DateTime)", {{"0", "0", "0", DateTime_Match_DATA}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(DateTime)", {{"0", "0", "0", DateTime_Match_DATA}}};
    }
    case Test_MyDateTime:
    {
        return {"MyDateTime", {{"0", "0", "0", MyDateTime_Match_DATE}}};
    }
    case Test_Nullable_MyDateTime:
    {
        if (has_null)
        {
            return {"Nullable(MyDateTime)", {{"0", "0", "0", MyDateTime_Match_DATE}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(MyDateTime)", {{"0", "0", "0", MyDateTime_Match_DATE}}};
    }
    case Test_Decimal64:
    {
        return {"Decimal(20, 5)", {{"0", "0", "0", Decimal_Match_DATA}}};
    }
    case Test_Nullable_Decimal64:
    {
        if (has_null)
        {
            return {"Nullable(Decimal(20, 5))", {{"0", "0", "0", Decimal_Match_DATA}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(Decimal(20, 5))", {{"0", "0", "0", Decimal_Match_DATA}}};
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateEqualOperator(MinMaxTestDatatype data_type, bool is_match)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        if (is_match)
        {
            return createEqual(attr("Int64"), Field(Int64_Match_DATA));
        }
        else
        {
            return createEqual(attr("Int64"), Field(Int64_Smaller_DATA));
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(Int64)"), Field(Int64_Match_DATA));
        }
        else
        {
            return createEqual(attr("Nullable(Int64)"), Field(Int64_Smaller_DATA));
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createEqual(attr("Date"), Field(Date_Match_DATA));
        }
        else
        {
            return createEqual(attr("Date"), Field(Date_Smaller_DATA));
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(Date)"), Field(Date_Match_DATA));
        }
        else
        {
            return createEqual(attr("Nullable(Date)"), Field(Date_Smaller_DATA));
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createEqual(attr("DateTime"), Field(DateTime_Match_DATA));
        }
        else
        {
            return createEqual(attr("DateTime"), Field(DateTime_Smaller_DATA));
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(DateTime)"), Field(DateTime_Match_DATA));
        }
        else
        {
            return createEqual(attr("Nullable(DateTime)"), Field(DateTime_Smaller_DATA));
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Match_DATE)));
        }
        else
        {
            return createEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Match_DATE)));
        }
        else
        {
            return createEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createEqual(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createEqual(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createEqual(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createEqual(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateInOperator(MinMaxTestDatatype data_type, bool is_match)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        if (is_match)
        {
            return createIn(attr("Int64"), {Field(Int64_Match_DATA)});
        }
        else
        {
            return createIn(attr("Int64"), {Field(Int64_Smaller_DATA)});
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(Int64)"), {Field(Int64_Match_DATA)});
        }
        else
        {
            return createIn(attr("Nullable(Int64)"), {Field(Int64_Smaller_DATA)});
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createIn(attr("Date"), {Field(Date_Match_DATA)});
        }
        else
        {
            return createIn(attr("Date"), {Field(Date_Smaller_DATA)});
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(Date)"), {Field(Date_Match_DATA)});
        }
        else
        {
            return createIn(attr("Nullable(Date)"), {Field(Date_Smaller_DATA)});
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createIn(attr("DateTime"), {Field(DateTime_Match_DATA)});
        }
        else
        {
            return createIn(attr("DateTime"), {Field(DateTime_Smaller_DATA)});
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(DateTime)"), {Field(DateTime_Match_DATA)});
        }
        else
        {
            return createIn(attr("Nullable(DateTime)"), {Field(DateTime_Smaller_DATA)});
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createIn(attr("MyDateTime"), {Field(parseMyDateTime(MyDateTime_Match_DATE))});
        }
        else
        {
            return createIn(attr("MyDateTime"), {Field(parseMyDateTime(MyDateTime_Smaller_DATE))});
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(MyDateTime)"), {Field(parseMyDateTime(MyDateTime_Match_DATE))});
        }
        else
        {
            return createIn(attr("Nullable(MyDateTime)"), {Field(parseMyDateTime(MyDateTime_Smaller_DATE))});
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createIn(
                attr("Decimal(20,5)"),
                {Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5))});
        }
        else
        {
            return createIn(
                attr("Decimal(20,5)"),
                {Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5))});
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createIn(
                attr("Nullable(Decimal(20,5))"),
                {Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5))});
        }
        else
        {
            return createIn(
                attr("Nullable(Decimal(20,5))"),
                {Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5))});
        }
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateGreaterOperator(MinMaxTestDatatype data_type, bool is_match)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        if (is_match)
        {
            return createGreater(attr("Int64"), Field(Int64_Smaller_DATA));
        }
        else
        {
            return createGreater(attr("Int64"), Field(Int64_Match_DATA));
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(Int64)"), Field(Int64_Smaller_DATA));
        }
        else
        {
            return createGreater(attr("Nullable(Int64)"), Field(Int64_Match_DATA));
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createGreater(attr("Date"), Field(Date_Smaller_DATA));
        }
        else
        {
            return createGreater(attr("Date"), Field(Date_Match_DATA));
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(Date)"), Field(Date_Smaller_DATA));
        }
        else
        {
            return createGreater(attr("Nullable(Date)"), Field(Date_Match_DATA));
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createGreater(attr("DateTime"), Field(DateTime_Smaller_DATA));
        }
        else
        {
            return createGreater(attr("DateTime"), Field(DateTime_Match_DATA));
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(DateTime)"), Field(DateTime_Smaller_DATA));
        }
        else
        {
            return createGreater(attr("Nullable(DateTime)"), Field(DateTime_Match_DATA));
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createGreater(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
        else
        {
            return createGreater(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Match_DATE)));
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
        else
        {
            return createGreater(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Match_DATE)));
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createGreater(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createGreater(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createGreater(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createGreater(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateGreaterEqualOperator(MinMaxTestDatatype data_type, bool is_match)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Int64"), Field(Int64_Smaller_DATA));
        }
        else
        {
            return createGreaterEqual(attr("Int64"), Field(Int64_Greater_DATA));
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(Int64)"), Field(Int64_Smaller_DATA));
        }
        else
        {
            return createGreaterEqual(attr("Nullable(Int64)"), Field(Int64_Greater_DATA));
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Date"), Field(Date_Smaller_DATA));
        }
        else
        {
            return createGreaterEqual(attr("Date"), Field(Date_Greater_DATA));
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(Date)"), Field(Date_Smaller_DATA));
        }
        else
        {
            return createGreaterEqual(attr("Nullable(Date)"), Field(Date_Greater_DATA));
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("DateTime"), Field(DateTime_Smaller_DATA));
        }
        else
        {
            return createGreaterEqual(attr("DateTime"), Field(DateTime_Greater_DATA));
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(DateTime)"), Field(DateTime_Smaller_DATA));
        }
        else
        {
            return createGreaterEqual(attr("Nullable(DateTime)"), Field(DateTime_Greater_DATA));
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
        else
        {
            return createGreaterEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Greater_DATE)));
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
        else
        {
            return createGreaterEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Greater_DATE)));
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createGreaterEqual(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createGreaterEqual(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createGreaterEqual(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createGreaterEqual(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateLessOperator(MinMaxTestDatatype data_type, bool is_match)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        if (is_match)
        {
            return createLess(attr("Int64"), Field(Int64_Greater_DATA));
        }
        else
        {
            return createLess(attr("Int64"), Field(Int64_Match_DATA));
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(Int64)"), Field(Int64_Greater_DATA));
        }
        else
        {
            return createLess(attr("Nullable(Int64)"), Field(Int64_Match_DATA));
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createLess(attr("Date"), Field(Date_Greater_DATA));
        }
        else
        {
            return createLess(attr("Date"), Field(Date_Match_DATA));
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(Date)"), Field(Date_Greater_DATA));
        }
        else
        {
            return createLess(attr("Nullable(Date)"), Field(Date_Match_DATA));
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createLess(attr("DateTime"), Field(DateTime_Greater_DATA));
        }
        else
        {
            return createLess(attr("DateTime"), Field(DateTime_Match_DATA));
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(DateTime)"), Field(DateTime_Greater_DATA));
        }
        else
        {
            return createLess(attr("Nullable(DateTime)"), Field(DateTime_Match_DATA));
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createLess(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Greater_DATE)));
        }
        else
        {
            return createLess(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Match_DATE)));
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Greater_DATE)));
        }
        else
        {
            return createLess(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Match_DATE)));
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createLess(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createLess(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createLess(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createLess(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateLessEqualOperator(MinMaxTestDatatype data_type, bool is_match)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        if (is_match)
        {
            return createLessEqual(attr("Int64"), Field(Int64_Greater_DATA));
        }
        else
        {
            return createLessEqual(attr("Int64"), Field(Int64_Smaller_DATA));
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(Int64)"), Field(Int64_Greater_DATA));
        }
        else
        {
            return createLessEqual(attr("Nullable(Int64)"), Field(Int64_Smaller_DATA));
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createLessEqual(attr("Date"), Field(Date_Greater_DATA));
        }
        else
        {
            return createLessEqual(attr("Date"), Field(Date_Smaller_DATA));
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(Date)"), Field(Date_Greater_DATA));
        }
        else
        {
            return createLessEqual(attr("Nullable(Date)"), Field(Date_Smaller_DATA));
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("DateTime"), Field(DateTime_Greater_DATA));
        }
        else
        {
            return createLessEqual(attr("DateTime"), Field(DateTime_Smaller_DATA));
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(DateTime)"), Field(DateTime_Greater_DATA));
        }
        else
        {
            return createLessEqual(attr("Nullable(DateTime)"), Field(DateTime_Smaller_DATA));
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Greater_DATE)));
        }
        else
        {
            return createLessEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Greater_DATE)));
        }
        else
        {
            return createLessEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)));
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createLessEqual(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createLessEqual(
                attr("Decimal(20,5)"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createLessEqual(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createLessEqual(
                attr("Nullable(Decimal(20,5))"),
                Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateIsNullOperator(MinMaxTestDatatype data_type)
{
    switch (data_type)
    {
    case Test_Int64:
    {
        return createIsNull(attr("Int64"));
    }
    case Test_Nullable_Int64:
    {
        return createIsNull(attr("Nullable(Int64)"));
    }
    case Test_Date:
    {
        return createIsNull(attr("Date"));
    }
    case Test_Nullable_Date:
    {
        return createIsNull(attr("Nullable(Date)"));
    }
    case Test_DateTime:
    {
        return createIsNull(attr("DateTime"));
    }
    case Test_Nullable_DateTime:
    {
        return createIsNull(attr("Nullable(DateTime)"));
    }
    case Test_MyDateTime:
    {
        return createIsNull(attr("MyDateTime"));
    }
    case Test_Nullable_MyDateTime:
    {
        return createIsNull(attr("Nullable(MyDateTime)"));
    }
    case Test_Decimal64:
    {
        return createIsNull(attr("Decimal(20,5)"));
    }
    case Test_Nullable_Decimal64:
    {
        return createIsNull(attr("Nullable(Decimal(20,5))"));
    }
    default:
        throw Exception("Unknown data type");
    }
}

RSOperatorPtr generateRSOperator(MinMaxTestDatatype data_type, MinMaxTestOperator rs_operator, bool is_match)
{
    switch (rs_operator)
    {
    case Test_Equal:
    {
        return generateEqualOperator(data_type, is_match);
    }
    case Test_In:
    {
        return generateInOperator(data_type, is_match);
    }
    case Test_Greater:
    {
        return generateGreaterOperator(data_type, is_match);
    }
    case Test_GreaterEqual:
    {
        return generateGreaterEqualOperator(data_type, is_match);
    }
    case Test_Less:
    {
        return generateLessOperator(data_type, is_match);
    }
    case Test_LessEqual:
    {
        return generateLessEqualOperator(data_type, is_match);
    }
    case Test_IsNull:
    {
        return generateIsNullOperator(data_type);
    }
    default:
        throw Exception("Unknown filter operator type");
    }
}
} // namespace

TEST_F(MinMaxIndexTest, Equal)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();

    for (size_t operater_type = Test_Equal; operater_type < Test_MaxOperator; operater_type++)
    {
        for (size_t datatype = Test_Int64; datatype < Test_Decimal64; datatype++)
        {
            {
                // not null
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true)));
                ASSERT_EQ(
                    false,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false)));
            }
            {
                // has null
                if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                {
                    continue;
                }
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true)));
                ASSERT_EQ(
                    false,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false)));
            }
        }
        // datatypes which not support minmax index
        for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
        {
            {
                // not null
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true)));
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false)));
            }
            {
                // has null
                if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                {
                    continue;
                }
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true)));
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false)));
            }
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, Not)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();

    for (size_t operater_type = Test_Equal; operater_type < Test_MaxOperator; operater_type++)
    {
        for (size_t datatype = Test_Int64; datatype < Test_Decimal64; datatype++)
        {
            {
                // not null
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                ASSERT_EQ(
                    false,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true))));
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false))));
            }
            {
                // has null
                if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                {
                    continue;
                }
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                ASSERT_EQ(
                    false,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true))));
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false))));
            }
        }

        // datatypes which not support minmax index
        for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
        {
            {
                // not null
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true))));
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false))));
            }
            {
                // has null
                if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                {
                    continue;
                }
                auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            true))));
                ASSERT_EQ(
                    true,
                    checkMatch(
                        case_name,
                        *context,
                        type_value_pair.first,
                        type_value_pair.second,
                        createNot(generateRSOperator(
                            static_cast<MinMaxTestDatatype>(datatype),
                            static_cast<MinMaxTestOperator>(operater_type),
                            false))));
            }
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, And)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();

    for (size_t operater_left_type = Test_Equal; operater_left_type < Test_MaxOperator; operater_left_type++)
    {
        for (size_t operater_right_type = Test_Equal; operater_right_type < Test_MaxOperator; operater_right_type++)
        {
            for (size_t datatype = Test_Int64; datatype < Test_Decimal64; datatype++)
            {
                {
                    // not null
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        true);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        true);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator})));

                    auto right_rs_operator_not_match = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);
                    ASSERT_EQ(
                        false,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator_not_match})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        true);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        true);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator})));

                    auto right_rs_operator_not_match = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);
                    ASSERT_EQ(
                        false,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator_not_match})));
                }
            }
            // datatypes which not support minmax index
            for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
            {
                {
                    // not null
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        true);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        true);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator})));

                    auto right_rs_operator_not_match = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator_not_match})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        true);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        true);

                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator})));

                    auto right_rs_operator_not_match = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createAnd({left_rs_operator, right_rs_operator_not_match})));
                }
            }
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, Or)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();

    for (size_t operater_left_type = Test_Equal; operater_left_type < Test_MaxOperator; operater_left_type++)
    {
        for (size_t operater_right_type = Test_Equal; operater_right_type < Test_MaxOperator; operater_right_type++)
        {
            for (size_t datatype = Test_Int64; datatype < Test_Decimal64; datatype++)
            {
                {
                    // not null
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        true);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createOr({left_rs_operator, right_rs_operator})));

                    auto left_rs_operator_not_match = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        false);
                    ASSERT_EQ(
                        false,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createOr({left_rs_operator_not_match, right_rs_operator})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        true);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createOr({left_rs_operator, right_rs_operator})));

                    auto left_rs_operator_not_match = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        false);
                    ASSERT_EQ(
                        false,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createOr({left_rs_operator_not_match, right_rs_operator})));
                }
            }
            // datatypes which not support minmax index
            for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
            {
                {
                    // not null
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        false);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);
                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createOr({left_rs_operator, right_rs_operator})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
                    auto left_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_left_type),
                        false);
                    auto right_rs_operator = generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_right_type),
                        false);

                    ASSERT_EQ(
                        true,
                        checkMatch(
                            case_name,
                            *context,
                            type_value_pair.first,
                            type_value_pair.second,
                            createOr({left_rs_operator, right_rs_operator})));
                }
            }
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, IsNull)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();

    auto operater_type = Test_IsNull;

    for (size_t datatype = Test_Int64; datatype < Test_Decimal64; datatype++)
    {
        {
            // not null
            auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
            ASSERT_EQ(
                false,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        true)));
            ASSERT_EQ(
                false,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        false)));
        }
        {
            // has null
            if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
            {
                continue;
            }
            auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
            ASSERT_EQ(
                true,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        true)));
            ASSERT_EQ(
                true,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        false)));
        }
    }

    // datatypes which not support minmax index
    for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
    {
        {
            // not null
            auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), false);
            ASSERT_EQ(
                true,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        true)));
            ASSERT_EQ(
                true,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        false)));
        }
        {
            // has null
            if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
            {
                continue;
            }
            auto type_value_pair = generateTypeValue(static_cast<MinMaxTestDatatype>(datatype), true);
            ASSERT_EQ(
                true,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        true)));
            ASSERT_EQ(
                true,
                checkMatch(
                    case_name,
                    *context,
                    type_value_pair.first,
                    type_value_pair.second,
                    generateRSOperator(
                        static_cast<MinMaxTestDatatype>(datatype),
                        static_cast<MinMaxTestOperator>(operater_type),
                        false)));
        }
    }
}
CATCH


TEST_F(MinMaxIndexTest, checkPKMatch)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createEqual(pkAttr(), Field((Int64)100)), true));
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createGreater(pkAttr(), Field((Int64)99)), true));
    ASSERT_EQ(
        true,
        checkPkMatch(case_name, *context, "Int64", "100", createGreater(pkAttr(), Field((Int64)99)), false));
}
CATCH

TEST_F(MinMaxIndexTest, DelMark)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    ASSERT_EQ(
        true,
        checkMatch(
            case_name,
            *context,
            "Int64",
            {{"0", "0", "0", "100"}},
            createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(
        false,
        checkMatch(
            case_name,
            *context,
            "Int64",
            {{"0", "0", "1", "100"}},
            createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(
        true,
        checkMatch(
            case_name,
            *context,
            "Int64",
            {{"0", "0", "1", "100"}, {"1", "1", "0", "100"}},
            createGreaterEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(
        false,
        checkMatch(
            case_name,
            *context,
            "Int64",
            {{"0", "0", "1", "88"}, {"1", "1", "0", "100"}},
            createLess(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(false, checkDelMatch(case_name, *context, "Int64", "100", createEqual(attr("Int64"), Field((Int64)100))));

    ASSERT_EQ(
        true,
        checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "\\N"}}, createIsNull(attr("Int64"))));
    ASSERT_EQ(false, checkDelMatch(case_name, *context, "Nullable(Int64)", "\\N", createIsNull(attr("Int64"))));
}
CATCH

TEST_F(MinMaxIndexTest, Enum8ValueCompare)
try
{
    DataTypeEnum8::Values values;
    values.push_back({"test", 50});
    values.push_back({"test_2", 100});
    values.push_back({"test_3", 0});
    auto enum8_type = std::make_shared<DataTypeEnum8>(values);
    ASSERT_EQ(
        RoughCheck::Cmp<EqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_2"), enum8_type, (Int8)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_3"), enum8_type, (Int8)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)49),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOp>::compare(Field((String) "test"), enum8_type, (Int8)49),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)49),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test_3"), enum8_type, (Int8)-1),
        ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOp>::compare(Field((String) "test"), enum8_type, (Int8)51), ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)51),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test_2"), enum8_type, (Int8)101),
        ValueCompareResult::True);
}
CATCH

TEST_F(MinMaxIndexTest, Enum16ValueCompare)
try
{
    DataTypeEnum16::Values values;
    values.push_back({"test", 50});
    values.push_back({"test_2", 100});
    values.push_back({"test_3", 0});
    auto enum16_type = std::make_shared<DataTypeEnum16>(values);
    ASSERT_EQ(
        RoughCheck::Cmp<EqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_2"), enum16_type, (Int16)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_3"), enum16_type, (Int16)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)49),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOp>::compare(Field((String) "test"), enum16_type, (Int16)49),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)49),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test_3"), enum16_type, (Int16)-1),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<LessOp>::compare(Field((String) "test"), enum16_type, (Int16)51),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)51),
        ValueCompareResult::True);
    ASSERT_EQ(
        RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test_2"), enum16_type, (Int16)101),
        ValueCompareResult::True);
}
CATCH

// Check it compatible with the minmax index generated by the version before v6.4
TEST_F(MinMaxIndexTest, CompatibleOldMinmaxIndex)
try
{
    RSCheckParam param;

    auto type = std::make_shared<DataTypeInt64>();
    auto data_type = makeNullable(type);

    // Generate a minmax index with the min value is null as a old version(before v6.4) minmax index.
    PaddedPODArray<UInt8> has_null_marks(1);
    PaddedPODArray<UInt8> has_value_marks(1);
    MutableColumnPtr minmaxes = data_type->createColumn();

    auto column = data_type->createColumn();

    column->insert(Field(static_cast<Int64>(1))); // insert value 1
    column->insertDefault(); // insert null value

    auto * col = column.get();
    minmaxes->insertFrom(*col, 1); // insert min index
    minmaxes->insertFrom(*col, 0); // insert max index

    auto minmax
        = std::make_shared<MinMaxIndex>(std::move(has_null_marks), std::move(has_value_marks), std::move(minmaxes));

    auto index = RSIndex(data_type, minmax);
    param.indexes.emplace(DEFAULT_COL_ID, index);

    // make a euqal filter, check equal with 1
    auto filter = createEqual(attr("Nullable(Int64)"), Field(static_cast<Int64>(1)));

    ASSERT_EQ(filter->roughCheck(0, 1, param)[0], RSResult::SomeNull);
}
CATCH

TEST_F(MinMaxIndexTest, InOrNotInNULL)
try
{
    RSCheckParam param;

    auto type = std::make_shared<DataTypeInt64>();
    auto data_type = makeNullable(type);

    PaddedPODArray<UInt8> has_null_marks(1);
    PaddedPODArray<UInt8> has_value_marks(1);
    MutableColumnPtr minmaxes = data_type->createColumn();

    auto column = data_type->createColumn();

    column->insert(Field(static_cast<Int64>(1))); // insert value 1
    column->insert(Field(static_cast<Int64>(2))); // insert value 2
    column->insertDefault(); // insert null value

    auto * col = column.get();
    minmaxes->insertFrom(*col, 0); // insert min index
    minmaxes->insertFrom(*col, 1); // insert max index

    auto minmax
        = std::make_shared<MinMaxIndex>(std::move(has_null_marks), std::move(has_value_marks), std::move(minmaxes));

    auto index = RSIndex(data_type, minmax);
    param.indexes.emplace(DEFAULT_COL_ID, index);

    {
        // make a in filter, check in (NULL)
        auto filter = createIn(attr("Nullable(Int64)"), {Field()});
        ASSERT_EQ(filter->roughCheck(0, 1, param)[0], RSResult::NoneNull);
    }
    {
        // make a in filter, check in (NULL, 1)
        auto filter = createIn(attr("Nullable(Int64)"), {Field(), Field(static_cast<Int64>(1))});
        ASSERT_EQ(filter->roughCheck(0, 1, param)[0], RSResult::SomeNull);
    }
    {
        // make a in filter, check in (3)
        auto filter = createIn(attr("Nullable(Int64)"), {Field(static_cast<Int64>(3))});
        ASSERT_EQ(filter->roughCheck(0, 1, param)[0], RSResult::NoneNull);
    }
    {
        // make a not in filter, check not in (NULL)
        auto filter = createNot(createIn(attr("Nullable(Int64)"), {Field()}));
        ASSERT_EQ(filter->roughCheck(0, 1, param)[0], RSResult::AllNull);
    }
    {
        // make a not in filter, check not in (NULL, 1)
        auto filter = createNot(createIn(attr("Nullable(Int64)"), {Field(), Field(static_cast<Int64>(1))}));
        ASSERT_EQ(filter->roughCheck(0, 1, param)[0], RSResult::SomeNull);
    }
    {
        // make a not in filter, check not in (3)
        auto filter = createNot(createIn(attr("Nullable(Int64)"), {Field(static_cast<Int64>(3))}));
        ASSERT_EQ(filter->roughCheck(0, 1, param)[0], RSResult::AllNull);
    }
}
CATCH

TEST_F(MinMaxIndexTest, ParseIn)
try
{
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{};
    google::protobuf::RepeatedPtrField<tipb::Expr> filters;
    {
        // a in (1, 2)
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::InInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        {
            tipb::Expr * col = expr.add_children();
            col->set_tp(tipb::ExprType::ColumnRef);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(1, ss);
                col->set_val(ss.releaseStr());
            }
            auto * field_type = col->mutable_field_type();
            field_type->set_tp(tipb::ExprType::Int64);
            field_type->set_flag(0);
        }
        {
            tipb::Expr * lit = expr.add_children();
            lit->set_tp(tipb::ExprType::Int64);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(1, ss);
                lit->set_val(ss.releaseStr());
            }
        }
        {
            tipb::Expr * lit = expr.add_children();
            lit->set_tp(tipb::ExprType::Int64);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(2, ss);
                lit->set_val(ss.releaseStr());
            }
        }
        filters.Add()->CopyFrom(expr);
    }
    {
        // a in (1, b)
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::InInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        {
            tipb::Expr * col = expr.add_children();
            col->set_tp(tipb::ExprType::ColumnRef);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(1, ss);
                col->set_val(ss.releaseStr());
            }
            auto * field_type = col->mutable_field_type();
            field_type->set_tp(tipb::ExprType::Int64);
            field_type->set_flag(0);
        }
        {
            tipb::Expr * lit = expr.add_children();
            lit->set_tp(tipb::ExprType::Int64);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(1, ss);
                lit->set_val(ss.releaseStr());
            }
        }
        {
            tipb::Expr * col = expr.add_children();
            col->set_tp(tipb::ExprType::ColumnRef);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(2, ss);
                col->set_val(ss.releaseStr());
            }
            auto * field_type = col->mutable_field_type();
            field_type->set_tp(tipb::ExprType::Int64);
            field_type->set_flag(0);
        }
        filters.Add()->CopyFrom(expr);
    }
    {
        // a in (b), this will not really happen, and it will be optimized to a = b
        // just for test
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::InInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        {
            tipb::Expr * col = expr.add_children();
            col->set_tp(tipb::ExprType::ColumnRef);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(1, ss);
                col->set_val(ss.releaseStr());
            }
            auto * field_type = col->mutable_field_type();
            field_type->set_tp(tipb::ExprType::Int64);
            field_type->set_flag(0);
        }
        {
            tipb::Expr * col = expr.add_children();
            col->set_tp(tipb::ExprType::ColumnRef);
            {
                WriteBufferFromOwnString ss;
                encodeDAGInt64(2, ss);
                col->set_val(ss.releaseStr());
            }
            auto * field_type = col->mutable_field_type();
            field_type->set_tp(tipb::ExprType::Int64);
            field_type->set_flag(0);
        }
        filters.Add()->CopyFrom(expr);
    }

    const ColumnDefines columns_to_read
        = {ColumnDefine{1, "a", std::make_shared<DataTypeInt64>()},
           ColumnDefine{2, "b", std::make_shared<DataTypeInt64>()}};
    // Only need id of ColumnInfo
    TiDB::ColumnInfo a, b;
    a.id = 1;
    b.id = 2;
    TiDB::ColumnInfos column_infos = {a, b};
    auto dag_query = std::make_unique<DAGQueryInfo>(
        filters,
        pushed_down_filters, // Not care now
        column_infos,
        std::vector<int>{},
        0,
        context->getTimezoneInfo());
    auto create_attr_by_column_id = [&columns_to_read](ColumnID column_id) -> Attr {
        auto iter
            = std::find_if(columns_to_read.begin(), columns_to_read.end(), [column_id](const ColumnDefine & d) -> bool {
                  return d.id == column_id;
              });
        if (iter != columns_to_read.end())
            return Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
        return Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
    };
    const auto op
        = DB::DM::FilterParser::parseDAGQuery(*dag_query, column_infos, create_attr_by_column_id, Logger::get());
    EXPECT_EQ(
        op->toDebugString(),
        R"raw({"op":"and","children":[{"op":"in","col":"b","value":"["1","2"]},{"op":"unsupported","reason":"Multiple ColumnRef in expression is not supported, sig=InInt"},{"op":"unsupported","reason":"Multiple ColumnRef in expression is not supported, sig=InInt"}]})raw");
}
CATCH

namespace
{
// Only support Int64 for testing.
template <typename T>
MinMaxIndexPtr createMinMaxIndex(const IDataType & col_type, const T & cases)
{
    auto minmax_index = std::make_shared<MinMaxIndex>(col_type);
    for (const auto & c : cases)
    {
        RUNTIME_CHECK(c.column_data.size(), c.del_mark.size());
        auto col_data = createColumn<Nullable<Int64>>(c.column_data).column;
        auto del_mark_col = createColumn<UInt8>(c.del_mark).column;
        minmax_index->addPack(*col_data, static_cast<const ColumnVector<UInt8> *>(del_mark_col.get()));
    }
    return minmax_index;
}
} // namespace

TEST_F(MinMaxIndexTest, CheckIsNull)
try
{
    struct IsNullTestCase
    {
        std::vector<std::optional<Int64>> column_data;
        std::vector<UInt64> del_mark;
        RSResult result;
    };

    std::vector<IsNullTestCase> cases = {
        {{1, 2, 3, 4, std::nullopt}, {0, 0, 0, 0, 0}, RSResult::Some},
        {{6, 7, 8, 9, 10}, {0, 0, 0, 0, 0}, RSResult::None},
        {{std::nullopt, std::nullopt}, {0, 0}, RSResult::All},
        {{1, 2, 3, 4, std::nullopt}, {0, 0, 0, 0, 1}, RSResult::None},
        {{6, 7, 8, 9, 10}, {0, 0, 0, 1, 0}, RSResult::None},
        {{std::nullopt, std::nullopt}, {1, 0}, RSResult::All},
        {{std::nullopt, std::nullopt}, {1, 1}, RSResult::None},
        {{1, 2, 3, 4}, {1, 1, 1, 1}, RSResult::None},
    };

    auto col_type = makeNullable(std::make_shared<DataTypeInt64>());
    auto minmax_index = createMinMaxIndex(*col_type, cases);

    auto actual_results = minmax_index->checkIsNull(0, cases.size());
    for (size_t i = 0; i < cases.size(); ++i)
    {
        const auto & c = cases[i];
        ASSERT_EQ(actual_results[i], c.result)
            << fmt::format("i={} actual={} expected={}", i, actual_results[i], c.result);
    }
}
CATCH

namespace
{
struct MinMaxCheckTestData
{
    std::vector<std::optional<Int64>> column_data;
    std::vector<UInt64> del_mark;
};

const auto min_max_check_test_data = std::array{
    MinMaxCheckTestData{
        .column_data = {1, 2, 3, 4, std::nullopt},
        .del_mark = {0, 0, 0, 0, 0},
    },
    MinMaxCheckTestData{
        .column_data = {6, 7, 8, 9, 10},
        .del_mark = {0, 0, 0, 0, 0},
    },
    MinMaxCheckTestData{
        .column_data = {std::nullopt, std::nullopt},
        .del_mark = {0, 0},
    },
    MinMaxCheckTestData{
        .column_data = {1, 2, 3, 4, std::nullopt},
        .del_mark = {0, 0, 0, 0, 1},
    },
    MinMaxCheckTestData{
        .column_data = {6, 7, 8, 9, 10},
        .del_mark = {0, 0, 0, 1, 0},
    },
    MinMaxCheckTestData{
        .column_data = {std::nullopt, std::nullopt},
        .del_mark = {1, 0},
    },
    MinMaxCheckTestData{
        .column_data = {std::nullopt, std::nullopt},
        .del_mark = {1, 1},
    },
    MinMaxCheckTestData{
        .column_data = {1, 2, 3, 4},
        .del_mark = {1, 1, 1, 1},
    },
    MinMaxCheckTestData{
        .column_data = {1, 1},
        .del_mark = {0, 0},
    },
    MinMaxCheckTestData{
        .column_data = {1, 1, std::nullopt},
        .del_mark = {0, 0, 0},
    },
};
} // namespace

TEST_F(MinMaxIndexTest, CheckIn)
try
{
    struct ValuesAndResults
    {
        std::vector<Int64> values; // select ... in (values)
        std::array<RSResult, min_max_check_test_data.size()> results; // Result of each test data
    };

    std::vector<ValuesAndResults> params = {
        {
            .values = {1, 2, 3, 4, 5, 6},
            .results = {
                RSResult::SomeNull,  // checkIn can return All only when min value equals to max value
                RSResult::Some,
                RSResult::SomeNull,  // All the fields are null, the default value is null, meet the compatibility check
                RSResult::Some,  // checkIn can return All only when min value equals to max value
                RSResult::Some,
                RSResult::SomeNull,  // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull,  // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull,  // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::All,   // checkIn can return All only when min value equals to max value
                RSResult::AllNull, // checkIn can return All only when min value equals to max value
            },
        },
        {
            .values = {100},
            .results = {
            RSResult::NoneNull,
            RSResult::None,
            RSResult::SomeNull,  // All the fields are null, the default value is null, meet the compatibility check
            RSResult::None,
            RSResult::None,
            RSResult::SomeNull,  // All the fields are null, the default value is null, meet the compatibility check
            RSResult::SomeNull,  // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::SomeNull,  // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::None,
            RSResult::NoneNull,
            },
        },
        {
            .values = {0},
            .results = {
            RSResult::NoneNull,
            RSResult::None,
            RSResult::SomeNull,  // All the fields are null, the default value is null, meet the compatibility check
            RSResult::None,
            RSResult::None,
            RSResult::SomeNull,  // All the fields are null, the default value is null, meet the compatibility check
            RSResult::SomeNull,  // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::SomeNull,  // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::None,
            RSResult::NoneNull,
            },
        },
    };

    auto col_type = makeNullable(std::make_shared<DataTypeInt64>());
    auto minmax_index = createMinMaxIndex(*col_type, min_max_check_test_data);
    for (const auto & [values, expected_results] : params)
    {
        auto actual_results = minmax_index->checkIn(
            0,
            min_max_check_test_data.size(),
            std::vector<Field>(values.cbegin(), values.cend()),
            col_type);
        for (size_t j = 0; j < min_max_check_test_data.size(); ++j)
        {
            ASSERT_EQ(actual_results[j], expected_results[j]) << fmt::format(
                "<{}> column_data={}, del_mark={}, values={}, actual={} expected={}",
                j,
                min_max_check_test_data[j].column_data,
                min_max_check_test_data[j].del_mark,
                values,
                actual_results[j],
                expected_results[j]);
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, CheckCmp_Equal)
try
{
    struct ValuesAndResults
    {
        Int64 value; // select ... = value
        std::array<RSResult, min_max_check_test_data.size()> results; // Result of each test data
    };

    std::vector<ValuesAndResults> params = {
        {
            .value = 1,
            .results = {
                RSResult::SomeNull,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::Some,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::All,
                RSResult::AllNull,
            },
        },
        {
            .value = 5,
            .results = {
                RSResult::NoneNull,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::NoneNull,
            },
        }
    };

    auto col_type = makeNullable(std::make_shared<DataTypeInt64>());
    auto minmax_index = createMinMaxIndex(*col_type, min_max_check_test_data);
    for (const auto & [value, expected_results] : params)
    {
        auto actual_results
            = minmax_index->checkCmp<RoughCheck::CheckEqual>(0, min_max_check_test_data.size(), value, col_type);
        for (size_t j = 0; j < min_max_check_test_data.size(); ++j)
        {
            ASSERT_EQ(actual_results[j], expected_results[j]) << fmt::format(
                "<{}> column_data={}, del_mark={}, values={}, actual={} expected={}",
                j,
                min_max_check_test_data[j].column_data,
                min_max_check_test_data[j].del_mark,
                value,
                actual_results[j],
                expected_results[j]);
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, CheckCmp_Greater)
try
{
    struct ValuesAndResults
    {
        Int64 value; // select ... > value
        std::array<RSResult, min_max_check_test_data.size()> results; // Result of each test data
    };

    std::vector<ValuesAndResults> params = {
        {
            .value = 0,
            .results = {
                RSResult::AllNull,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::All,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::All,
                RSResult::AllNull,
            },
        },
        {
            .value = 5,
            .results = {
                RSResult::NoneNull,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::NoneNull,
            },
        },
        {
            .value = 11,
            .results = {
                RSResult::NoneNull,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::NoneNull,
            },
        },
    };

    auto col_type = makeNullable(std::make_shared<DataTypeInt64>());
    auto minmax_index = createMinMaxIndex(*col_type, min_max_check_test_data);
    for (const auto & [value, expected_results] : params)
    {
        auto actual_results
            = minmax_index->checkCmp<RoughCheck::CheckGreater>(0, min_max_check_test_data.size(), value, col_type);
        for (size_t j = 0; j < min_max_check_test_data.size(); ++j)
        {
            ASSERT_EQ(actual_results[j], expected_results[j]) << fmt::format(
                "<{}> column_data={}, del_mark={}, values={}, actual={} expected={}",
                j,
                min_max_check_test_data[j].column_data,
                min_max_check_test_data[j].del_mark,
                value,
                actual_results[j],
                expected_results[j]);
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, CheckCmp_GreaterEqual)
try
{
    struct ValuesAndResults
    {
        Int64 value; // select ... >= value
        std::array<RSResult, min_max_check_test_data.size()> results; // Result of each test data
    };

    std::vector<ValuesAndResults> params = {
        {
            .value = 1,
            .results = {
				RSResult::AllNull,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::All,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::All,
                RSResult::AllNull,
            },
        },
        {
            .value = 2,
            .results = {
				RSResult::SomeNull,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::Some,
                RSResult::All,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::NoneNull,
            },
        },
        {
            .value = 10,
            .results = {
				RSResult::NoneNull,
                RSResult::Some,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::Some,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::NoneNull,
            },
        },
        {
            .value = 11,
            .results = {
				RSResult::NoneNull,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::None,
                RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
                RSResult::None,
                RSResult::NoneNull,
            },
        },
    };

    auto col_type = makeNullable(std::make_shared<DataTypeInt64>());
    auto minmax_index = createMinMaxIndex(*col_type, min_max_check_test_data);
    for (const auto & [value, expected_results] : params)
    {
        auto actual_results
            = minmax_index->checkCmp<RoughCheck::CheckGreaterEqual>(0, min_max_check_test_data.size(), value, col_type);
        for (size_t j = 0; j < min_max_check_test_data.size(); ++j)
        {
            ASSERT_EQ(actual_results[j], expected_results[j]) << fmt::format(
                "<{}> column_data={}, del_mark={}, values={}, actual={} expected={}",
                j,
                min_max_check_test_data[j].column_data,
                min_max_check_test_data[j].del_mark,
                value,
                actual_results[j],
                expected_results[j]);
        }
    }
}
CATCH

TEST_F(MinMaxIndexTest, CheckCmpNullValue)
try
{
    auto col_type = DataTypeFactory::instance().get("Int64");
    auto minmax_index = createMinMaxIndex(*col_type, min_max_check_test_data);
    auto rs_index = RSIndex(col_type, minmax_index);
    RSCheckParam param;
    param.indexes.emplace(DEFAULT_COL_ID, rs_index);

    Field null_value;
    ASSERT_TRUE(null_value.isNull());

    auto check = [&](RSOperatorPtr rs, RSResult expected_res) {
        auto results = rs->roughCheck(0, min_max_check_test_data.size(), param);
        for (auto actual_res : results)
            ASSERT_EQ(actual_res, expected_res) << fmt::format("actual: {}, expected: {}", actual_res, expected_res);
    };

    check(createGreater(attr("Int64"), null_value), RSResult::NoneNull);
    check(createGreaterEqual(attr("Int64"), null_value), RSResult::NoneNull);
    check(createLess(attr("Int64"), null_value), RSResult::AllNull);
    check(createLessEqual(attr("Int64"), null_value), RSResult::AllNull);
    check(createEqual(attr("Int64"), null_value), RSResult::NoneNull);
    check(createNotEqual(attr("Int64"), null_value), RSResult::AllNull);
}
CATCH

TEST_F(MinMaxIndexTest, CheckInNullValue)
try
{
    auto col_type = DataTypeFactory::instance().get("Nullable(Int64)");
    auto minmax_index = createMinMaxIndex(*col_type, min_max_check_test_data);
    auto rs_index = RSIndex(col_type, minmax_index);
    RSCheckParam param;
    param.indexes.emplace(DEFAULT_COL_ID, rs_index);

    Field null_value;
    ASSERT_TRUE(null_value.isNull());

    {
        auto rs = createIn(attr("Nullable(Int64)"), {null_value});
        auto results = rs->roughCheck(0, min_max_check_test_data.size(), param);
        auto excepted_results = {
            RSResult::NoneNull,
            RSResult::NoneNull,
            RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
            RSResult::NoneNull,
            RSResult::NoneNull,
            RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
            RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::NoneNull,
            RSResult::NoneNull,
        };
        ASSERT_TRUE(std::equal(results.cbegin(), results.cend(), excepted_results.begin()));
    }

    {
        auto rs = createIn(attr("Nullable(Int64)"), {Field{static_cast<Int64>(1)}, null_value});
        auto results = rs->roughCheck(0, min_max_check_test_data.size(), param);
        auto excepted_results = {
            RSResult::SomeNull,
            RSResult::NoneNull,
            RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
            RSResult::SomeNull,
            RSResult::NoneNull,
            RSResult::SomeNull, // All the fields are null, the default value is null, meet the compatibility check
            RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::SomeNull, // All the fields are deleted, the default value is null, meet the compatibility check
            RSResult::All,
            RSResult::AllNull,
        };
        ASSERT_TRUE(std::equal(results.cbegin(), results.cend(), excepted_results.begin()));
    }
}
CATCH

} // namespace DB::DM::tests
