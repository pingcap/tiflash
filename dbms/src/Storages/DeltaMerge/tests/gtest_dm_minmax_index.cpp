// Copyright 2022 PingCAP, Ltd.
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
#include <Interpreters/convertFieldToType.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>
#include <Storages/DeltaMerge/Index/ValueComparison.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
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
static const ColId DEFAULT_COL_ID = 0;
static const String DEFAULT_COL_NAME = "2020-09-26";

class DMMinMaxIndexTest : public ::testing::Test
{
public:
    DMMinMaxIndexTest() = default;

protected:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        context = std::make_unique<Context>(DMTestEnv::getContext());
        if (!context->getMinMaxIndexCache())
        {
            context->setMinMaxIndexCache(5368709120);
        }
    }

    void TearDown() override
    {
        context->dropMinMaxIndexCache();
    }

private:
protected:
    // a ptr to context, we can reload context with different settings if need.
    std::unique_ptr<Context> context;
};

Attr attr(String type)
{
    return Attr{DEFAULT_COL_NAME, DEFAULT_COL_ID, DataTypeFactory::instance().get(type)};
}

Attr pkAttr()
{
    const ColumnDefine & col = getExtraHandleColumnDefine(true);
    return Attr{col.name, col.id, col.type};
}

bool checkMatch(
    const String & test_case,
    Context & context,
    const String & type,
    const CSVTuples block_tuples,
    const RSOperatorPtr & filter,
    bool is_common_handle = false,
    bool check_pk = false)
{
    String name = "DMMinMaxIndexTest_" + test_case;

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

    DeltaMergeStorePtr store = std::make_shared<DeltaMergeStore>(
        context,
        false,
        "test_database",
        name,
        /*table_id*/ 100,
        table_columns,
        getExtraHandleColumnDefine(is_common_handle),
        is_common_handle,
        1);

    store->write(context, context.getSettingsRef(), block);
    store->flushCache(context, all_range);
    store->mergeDeltaAll(context);

    const ColumnDefine & col_to_read = check_pk ? getExtraHandleColumnDefine(is_common_handle) : cd;
    auto streams = store->read(context, context.getSettingsRef(), {col_to_read}, {all_range}, 1, std::numeric_limits<UInt64>::max(), filter, name);
    streams[0]->readPrefix();
    auto rows = streams[0]->read().rows();
    streams[0]->readSuffix();
    store->drop();

    return rows != 0;
}

bool checkMatch(const String & test_case, Context & context, const String & type, const String & value, const RSOperatorPtr & filter)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{"0", "0", "0", value}};
    return checkMatch(test_case, context, type, tuples, filter);
}

bool checkDelMatch(const String & test_case, Context & context, const String & type, const String & value, const RSOperatorPtr & filter)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{"0", "0", "1", value}};
    return checkMatch(test_case, context, type, tuples, filter);
}

bool checkPkMatch(const String & test_case, Context & context, const String & type, const String & pk_value, const RSOperatorPtr & filter, bool is_common_handle)
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
};

Decimal64 getDecimal64(String s)
{
    Decimal64 expected_default_value;
    ReadBufferFromString buf(s);
    readDecimalText(expected_default_value, buf, /*precision*/ 20, /*scale*/ 5);
    return expected_default_value;
}

#define Int64_Match_DATA (100)
#define Int64_Greater_DATA (10000)
#define Int64_Smaller_DATA (-1)

#define Date_Match_DATA ("2020-09-27")
#define Date_Greater_DATA ("2022-09-27")
#define Date_Smaller_DATA ("1997-09-27")

#define DateTime_Match_DATA ("2020-01-01 05:00:01")
#define DateTime_Greater_DATA ("2022-01-01 05:00:01")
#define DateTime_Smaller_DATA ("1997-01-01 05:00:01")

#define MyDateTime_Match_DATE ("2020-09-27")
#define MyDateTime_Greater_DATE ("2022-09-27")
#define MyDateTime_Smaller_DATE ("1997-09-27")

#define Decimal_Match_DATA ("100.25566")
#define Decimal_UnMatch_DATA ("100.25500")

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
        return {"Date", {{"0", "0", "0", DB::toString(Date_Match_DATA)}}};
    }
    case Test_Nullable_Date:
    {
        if (has_null)
        {
            return {"Nullable(Date)", {{"0", "0", "0", DB::toString(Date_Match_DATA)}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(Date)", {{"0", "0", "0", DB::toString(Date_Match_DATA)}}};
    }
    case Test_DateTime:
    {
        return {"DateTime", {{"0", "0", "0", DB::toString(DateTime_Match_DATA)}}};
    }
    case Test_Nullable_DateTime:
    {
        if (has_null)
        {
            return {"Nullable(DateTime)", {{"0", "0", "0", DB::toString(DateTime_Match_DATA)}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(DateTime)", {{"0", "0", "0", DB::toString(DateTime_Match_DATA)}}};
    }
    case Test_MyDateTime:
    {
        return {"MyDateTime", {{"0", "0", "0", DB::toString(MyDateTime_Match_DATE)}}};
    }
    case Test_Nullable_MyDateTime:
    {
        if (has_null)
        {
            return {"Nullable(MyDateTime)", {{"0", "0", "0", DB::toString(MyDateTime_Match_DATE)}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(MyDateTime)", {{"0", "0", "0", DB::toString(MyDateTime_Match_DATE)}}};
    }
    case Test_Decimal64:
    {
        return {"Decimal(20, 5)", {{"0", "0", "0", DB::toString(Decimal_Match_DATA)}}};
    }
    case Test_Nullable_Decimal64:
    {
        if (has_null)
        {
            return {"Nullable(Decimal(20, 5))", {{"0", "0", "0", DB::toString(Decimal_Match_DATA)}, {"1", "1", "0", "\\N"}}};
        }
        return {"Nullable(Decimal(20, 5))", {{"0", "0", "0", DB::toString(Decimal_Match_DATA)}}};
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
            return createEqual(attr("Int64"), Field(static_cast<Int64> Int64_Match_DATA));
        }
        else
        {
            return createEqual(attr("Int64"), Field(static_cast<Int64> Int64_Smaller_DATA));
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Match_DATA));
        }
        else
        {
            return createEqual(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Smaller_DATA));
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createEqual(attr("Date"), Field((String)Date_Match_DATA));
        }
        else
        {
            return createEqual(attr("Date"), Field((String)Date_Smaller_DATA));
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(Date)"), Field((String)Date_Match_DATA));
        }
        else
        {
            return createEqual(attr("Nullable(Date)"), Field((String)Date_Smaller_DATA));
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createEqual(attr("DateTime"), Field((String)DateTime_Match_DATA));
        }
        else
        {
            return createEqual(attr("DateTime"), Field((String)DateTime_Smaller_DATA));
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(DateTime)"), Field((String)DateTime_Match_DATA));
        }
        else
        {
            return createEqual(attr("Nullable(DateTime)"), Field((String)DateTime_Smaller_DATA));
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
            return createEqual(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createEqual(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createEqual(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)));
        }
        else
        {
            return createEqual(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)));
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
            return createIn(attr("Int64"), {Field(static_cast<Int64> Int64_Match_DATA)});
        }
        else
        {
            return createIn(attr("Int64"), {Field(static_cast<Int64> Int64_Smaller_DATA)});
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(Int64)"), {Field(static_cast<Int64> Int64_Match_DATA)});
        }
        else
        {
            return createIn(attr("Nullable(Int64)"), {Field(static_cast<Int64> Int64_Smaller_DATA)});
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createIn(attr("Date"), {Field((String)Date_Match_DATA)});
        }
        else
        {
            return createIn(attr("Date"), {Field((String)Date_Smaller_DATA)});
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(Date)"), {Field((String)Date_Match_DATA)});
        }
        else
        {
            return createIn(attr("Nullable(Date)"), {Field((String)Date_Smaller_DATA)});
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createIn(attr("DateTime"), {Field((String)DateTime_Match_DATA)});
        }
        else
        {
            return createIn(attr("DateTime"), {Field((String)DateTime_Smaller_DATA)});
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(DateTime)"), {Field((String)DateTime_Match_DATA)});
        }
        else
        {
            return createIn(attr("Nullable(DateTime)"), {Field((String)DateTime_Smaller_DATA)});
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
            return createIn(attr("Decimal(20,5)"), {Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5))});
        }
        else
        {
            return createIn(attr("Decimal(20,5)"), {Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5))});
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createIn(attr("Nullable(Decimal(20,5))"), {Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5))});
        }
        else
        {
            return createIn(attr("Nullable(Decimal(20,5))"), {Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5))});
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
            return createGreater(attr("Int64"), Field(static_cast<Int64> Int64_Smaller_DATA), 0);
        }
        else
        {
            return createGreater(attr("Int64"), Field(static_cast<Int64> Int64_Match_DATA), 0);
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Smaller_DATA), 0);
        }
        else
        {
            return createGreater(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Match_DATA), 0);
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createGreater(attr("Date"), Field((String)Date_Smaller_DATA), 0);
        }
        else
        {
            return createGreater(attr("Date"), Field((String)Date_Match_DATA), 0);
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(Date)"), Field((String)Date_Smaller_DATA), 0);
        }
        else
        {
            return createGreater(attr("Nullable(Date)"), Field((String)Date_Match_DATA), 0);
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createGreater(attr("DateTime"), Field((String)DateTime_Smaller_DATA), 0);
        }
        else
        {
            return createGreater(attr("DateTime"), Field((String)DateTime_Match_DATA), 0);
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(DateTime)"), Field((String)DateTime_Smaller_DATA), 0);
        }
        else
        {
            return createGreater(attr("Nullable(DateTime)"), Field((String)DateTime_Match_DATA), 0);
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createGreater(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)), 0);
        }
        else
        {
            return createGreater(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Match_DATE)), 0);
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)), 0);
        }
        else
        {
            return createGreater(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Match_DATE)), 0);
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createGreater(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createGreater(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createGreater(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createGreater(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
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
            return createGreaterEqual(attr("Int64"), Field(static_cast<Int64> Int64_Smaller_DATA), 0);
        }
        else
        {
            return createGreaterEqual(attr("Int64"), Field(static_cast<Int64> Int64_Greater_DATA), 0);
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Smaller_DATA), 0);
        }
        else
        {
            return createGreaterEqual(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Greater_DATA), 0);
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Date"), Field((String)Date_Smaller_DATA), 0);
        }
        else
        {
            return createGreaterEqual(attr("Date"), Field((String)Date_Greater_DATA), 0);
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(Date)"), Field((String)Date_Smaller_DATA), 0);
        }
        else
        {
            return createGreaterEqual(attr("Nullable(Date)"), Field((String)Date_Greater_DATA), 0);
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("DateTime"), Field((String)DateTime_Smaller_DATA), 0);
        }
        else
        {
            return createGreaterEqual(attr("DateTime"), Field((String)DateTime_Greater_DATA), 0);
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(DateTime)"), Field((String)DateTime_Smaller_DATA), 0);
        }
        else
        {
            return createGreaterEqual(attr("Nullable(DateTime)"), Field((String)DateTime_Greater_DATA), 0);
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)), 0);
        }
        else
        {
            return createGreaterEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Greater_DATE)), 0);
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)), 0);
        }
        else
        {
            return createGreaterEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Greater_DATE)), 0);
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createGreaterEqual(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createGreaterEqual(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createGreaterEqual(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
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
            return createLess(attr("Int64"), Field(static_cast<Int64> Int64_Greater_DATA), 0);
        }
        else
        {
            return createLess(attr("Int64"), Field(static_cast<Int64> Int64_Match_DATA), 0);
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Greater_DATA), 0);
        }
        else
        {
            return createLess(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Match_DATA), 0);
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createLess(attr("Date"), Field((String)Date_Greater_DATA), 0);
        }
        else
        {
            return createLess(attr("Date"), Field((String)Date_Match_DATA), 0);
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(Date)"), Field((String)Date_Greater_DATA), 0);
        }
        else
        {
            return createLess(attr("Nullable(Date)"), Field((String)Date_Match_DATA), 0);
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createLess(attr("DateTime"), Field((String)DateTime_Greater_DATA), 0);
        }
        else
        {
            return createLess(attr("DateTime"), Field((String)DateTime_Match_DATA), 0);
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(DateTime)"), Field((String)DateTime_Greater_DATA), 0);
        }
        else
        {
            return createLess(attr("Nullable(DateTime)"), Field((String)DateTime_Match_DATA), 0);
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createLess(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Greater_DATE)), 0);
        }
        else
        {
            return createLess(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Match_DATE)), 0);
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Greater_DATE)), 0);
        }
        else
        {
            return createLess(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Match_DATE)), 0);
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createLess(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createLess(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createLess(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createLess(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
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
            return createLessEqual(attr("Int64"), Field(static_cast<Int64> Int64_Greater_DATA), 0);
        }
        else
        {
            return createLessEqual(attr("Int64"), Field(static_cast<Int64> Int64_Smaller_DATA), 0);
        }
    }
    case Test_Nullable_Int64:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Greater_DATA), 0);
        }
        else
        {
            return createLessEqual(attr("Nullable(Int64)"), Field(static_cast<Int64> Int64_Smaller_DATA), 0);
        }
    }
    case Test_Date:
    {
        if (is_match)
        {
            return createLessEqual(attr("Date"), Field((String)Date_Greater_DATA), 0);
        }
        else
        {
            return createLessEqual(attr("Date"), Field((String)Date_Smaller_DATA), 0);
        }
    }
    case Test_Nullable_Date:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(Date)"), Field((String)Date_Greater_DATA), 0);
        }
        else
        {
            return createLessEqual(attr("Nullable(Date)"), Field((String)Date_Smaller_DATA), 0);
        }
    }
    case Test_DateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("DateTime"), Field((String)DateTime_Greater_DATA), 0);
        }
        else
        {
            return createLessEqual(attr("DateTime"), Field((String)DateTime_Smaller_DATA), 0);
        }
    }
    case Test_Nullable_DateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(DateTime)"), Field((String)DateTime_Greater_DATA), 0);
        }
        else
        {
            return createLessEqual(attr("Nullable(DateTime)"), Field((String)DateTime_Smaller_DATA), 0);
        }
    }
    case Test_MyDateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Greater_DATE)), 0);
        }
        else
        {
            return createLessEqual(attr("MyDateTime"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)), 0);
        }
    }
    case Test_Nullable_MyDateTime:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Greater_DATE)), 0);
        }
        else
        {
            return createLessEqual(attr("Nullable(MyDateTime)"), Field(parseMyDateTime(MyDateTime_Smaller_DATE)), 0);
        }
    }
    case Test_Decimal64:
    {
        if (is_match)
        {
            return createLessEqual(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createLessEqual(attr("Decimal(20,5)"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
        }
    }
    case Test_Nullable_Decimal64:
    {
        if (is_match)
        {
            return createLessEqual(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_Match_DATA), 5)), 0);
        }
        else
        {
            return createLessEqual(attr("Nullable(Decimal(20,5))"), Field(DecimalField<Decimal64>(getDecimal64(Decimal_UnMatch_DATA), 5)), 0);
        }
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
    default:
        throw Exception("Unknown filter operator type");
    }
}

TEST_F(DMMinMaxIndexTest, Equal)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();

    for (size_t operater_type = Test_Equal; operater_type < Test_MaxOperator; operater_type++)
    {
        for (size_t datatype = Test_Int64; datatype < Test_Decimal64; datatype++)
        {
            {
                // not null
                auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), false);
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), true)));
                ASSERT_EQ(false, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), false)));
            }
            {
                // has null
                if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                {
                    continue;
                }
                auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), true);
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), true)));
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), false)));
            }
        }
        // datatypes which not support minmax index
        for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
        {
            {
                // not null
                auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), false);
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), true)));
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), false)));
            }
            {
                // has null
                if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                {
                    continue;
                }
                auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), true);
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), true)));
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), false)));
            }
        }
    }
}
CATCH

TEST_F(DMMinMaxIndexTest, Not)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();

    for (size_t operater_type = Test_Equal; operater_type < Test_MaxOperator; operater_type++)
    {
        for (size_t datatype = Test_Int64; datatype < Test_Decimal64; datatype++)
        {
            {
                // not null
                auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), false);
                ASSERT_EQ(false, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createNot(generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), true))));
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createNot(generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), false))));
            }
            {
                // has null
                if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                {
                    continue;
                }
                auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), true);
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createNot(generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), true))));
                ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createNot(generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_type), false))));
            }
        }
    }
}
CATCH

TEST_F(DMMinMaxIndexTest, And)
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
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), false);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), true);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), true);
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator})));

                    auto right_rs_operator_not_match = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);
                    ASSERT_EQ(false, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator_not_match})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), true);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), true);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator})));
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator})));
                }
            }
            // datatypes which not support minmax index
            for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
            {
                {
                    // not null
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), false);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), true);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator})));
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), true);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), true);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);

                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator})));
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createAnd({left_rs_operator, right_rs_operator})));
                }
            }
        }
    }
}
CATCH

TEST_F(DMMinMaxIndexTest, Or)
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
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), false);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), true);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createOr({left_rs_operator, right_rs_operator})));

                    auto right_rs_operator_not_match = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createOr({left_rs_operator, right_rs_operator_not_match})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), true);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), true);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createOr({left_rs_operator, right_rs_operator})));
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createOr({left_rs_operator, right_rs_operator})));
                }
            }
            // datatypes which not support minmax index
            for (size_t datatype = Test_Decimal64; datatype < Test_Max; datatype++)
            {
                {
                    // not null
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), false);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), false);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);
                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createOr({left_rs_operator, right_rs_operator})));
                }
                {
                    // has null
                    if (!isNullableDateType(static_cast<MinMaxTestDatatype>(datatype)))
                    {
                        continue;
                    }
                    auto type_value_pair = generateTypeValue(MinMaxTestDatatype(datatype), true);
                    auto left_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_left_type), false);
                    auto right_rs_operator = generateRSOperator(static_cast<MinMaxTestDatatype>(datatype), static_cast<MinMaxTestOperator>(operater_right_type), false);

                    ASSERT_EQ(true, checkMatch(case_name, *context, type_value_pair.first, type_value_pair.second, createOr({left_rs_operator, right_rs_operator})));
                }
            }
        }
    }
}
CATCH

TEST_F(DMMinMaxIndexTest, checkPKMatch)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createEqual(pkAttr(), Field((Int64)100)), true));
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createGreater(pkAttr(), Field((Int64)99), 0), true));
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createGreater(pkAttr(), Field((Int64)99), 0), false));
}
CATCH

TEST_F(DMMinMaxIndexTest, DelMark)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", {{"0", "0", "0", "100"}}, createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", {{"0", "0", "1", "100"}}, createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(true,
              checkMatch(case_name,
                         *context,
                         "Int64",
                         {{"0", "0", "1", "100"}, {"1", "1", "0", "100"}},
                         createGreaterEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false,
              checkMatch(case_name,
                         *context,
                         "Int64",
                         {{"0", "0", "1", "88"}, {"1", "1", "0", "100"}},
                         createLess(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkDelMatch(case_name, *context, "Int64", "100", createEqual(attr("Int64"), Field((Int64)100))));
}
CATCH

TEST_F(DMMinMaxIndexTest, Enum8ValueCompare)
try
{
    DataTypeEnum8::Values values;
    values.push_back({"test", 50});
    values.push_back({"test_2", 100});
    values.push_back({"test_3", 0});
    auto enum8_type = std::make_shared<DataTypeEnum8>(values);
    ASSERT_EQ(RoughCheck::Cmp<EqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_2"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_3"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOp>::compare(Field((String) "test"), enum8_type, (Int8)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test_3"), enum8_type, (Int8)-1), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOp>::compare(Field((String) "test"), enum8_type, (Int8)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test_2"), enum8_type, (Int8)101), ValueCompareResult::True);
}
CATCH

TEST_F(DMMinMaxIndexTest, Enum16ValueCompare)
try
{
    DataTypeEnum16::Values values;
    values.push_back({"test", 50});
    values.push_back({"test_2", 100});
    values.push_back({"test_3", 0});
    auto enum16_type = std::make_shared<DataTypeEnum16>(values);
    ASSERT_EQ(RoughCheck::Cmp<EqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_2"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_3"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOp>::compare(Field((String) "test"), enum16_type, (Int16)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test_3"), enum16_type, (Int16)-1), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOp>::compare(Field((String) "test"), enum16_type, (Int16)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test_2"), enum16_type, (Int16)101), ValueCompareResult::True);
}
CATCH
} // namespace tests
} // namespace DM
} // namespace DB
