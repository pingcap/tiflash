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

#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <gtest/gtest.h>

#include <string>
#include <type_traits>

namespace DB::tests
{
/**
  * Because most functions(except cast string as json) have 
  *    ```
  *    bool useDefaultImplementationForNulls() const override { return true; }
  *    bool useDefaultImplementationForConstants() const override { return true; }
  *    ```
  * there is no need to test const, null_value, and only null.
  *
  * CastIntAsJson, CastStringAsJson and CastDurationAsJson can only test the case where input_tidb_tp/output_tidb_tp is nullptr
  */
class TestCastAsJson : public DB::tests::FunctionTest
{
public:
    template <bool is_raw = false>
    ColumnWithTypeAndName executeFunctionWithCast(const String & func_name, const ColumnsWithTypeAndName & columns)
    {
        ColumnWithTypeAndName json_column;
        if constexpr (is_raw)
        {
            json_column = executeFunction(func_name, columns, nullptr, true);
        }
        else
        {
            json_column = executeFunction(func_name, columns);
        }
        // The `json_binary` should be cast as a string to improve readability.
        tipb::FieldType field_type;
        field_type.set_flen(-1);
        field_type.set_collate(TiDB::ITiDBCollator::BINARY);
        field_type.set_tp(TiDB::TypeString);
        return executeCastJsonAsStringFunction(json_column, field_type);
    }

    template <typename Input, bool is_raw = false>
    void executeAndAssert(const String & func_name, const Input & input, const String & expect)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({expect}),
            executeFunctionWithCast<is_raw>(func_name, {createColumn<Input>({input})}));
    }

    template <typename Input, bool is_raw = false>
    typename std::enable_if<IsDecimal<Input>, void>::type executeAndAssert(
        const String & func_name,
        const DecimalField<Input> & input,
        const String & expect)
    {
        auto meta = std::make_tuple(19, input.getScale());
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({expect}),
            executeFunctionWithCast<is_raw>(func_name, {createColumn<Input>(meta, {input})}));
    }

    template <typename IntType>
    typename std::enable_if<std::is_integral_v<IntType>, void>::type testForInt()
    {
        // Only raw function test is tested, so input_tidb_tp is always nullptr.
        String func_name = "cast_int_as_json";
        executeAndAssert<IntType, true>(func_name, 0, "0");
        executeAndAssert<IntType, true>(func_name, 99, "99");
        if constexpr (std::is_signed_v<IntType>)
        {
            executeAndAssert<IntType, true>(func_name, -99, "-99");
        }
        executeAndAssert<IntType, true>(
            func_name,
            std::numeric_limits<IntType>::max(),
            fmt::format("{}", std::numeric_limits<IntType>::max()));
        executeAndAssert<IntType, true>(
            func_name,
            std::numeric_limits<IntType>::min(),
            fmt::format("{}", std::numeric_limits<IntType>::min()));
        executeAndAssert<IntType, true>(
            func_name,
            std::numeric_limits<IntType>::lowest(),
            fmt::format("{}", std::numeric_limits<IntType>::lowest()));
    }
};

TEST_F(TestCastAsJson, CastJsonAsJson)
try
{
    /// prepare
    // []
    // clang-format off
    const UInt8 empty_array[] = {
        JsonBinary::TYPE_CODE_ARRAY, // array_type
        0x0, 0x0, 0x0, 0x0, // element_count
        0x8, 0x0, 0x0, 0x0}; // total_size
    // clang-format on
    ColumnWithTypeAndName json_column;
    {
        auto empty_array_json = ColumnString::create();
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        json_column = ColumnWithTypeAndName(std::move(empty_array_json), std::make_shared<DataTypeString>());
    }

    auto gen_column_expect = [](const String & value) {
        // double for rows_count 2.
        return createColumn<Nullable<String>>({value, value});
    };

    auto res = executeFunctionWithCast("cast_json_as_json", {json_column});
    auto expect = gen_column_expect("[]");
    ASSERT_COLUMN_EQ(expect, res);
}
CATCH

TEST_F(TestCastAsJson, CastIntAsJson)
try
{
    testForInt<UInt8>();
    testForInt<UInt16>();
    testForInt<UInt32>();
    testForInt<UInt64>();

    testForInt<Int8>();
    testForInt<Int16>();
    testForInt<Int32>();
    testForInt<Int64>();
}
CATCH

TEST_F(TestCastAsJson, CastRealAsJson)
try
{
    const String func_name = "cast_real_as_json";

    /// Float32
    executeAndAssert<Float32>(func_name, 0, "0");
    executeAndAssert<Float32>(func_name, 999.999f, "999.9990234375");
    executeAndAssert<Float32>(func_name, -999.999f, "-999.9990234375");
    executeAndAssert<Float32>(
        func_name,
        std::numeric_limits<Float32>::max(),
        fmt::format("{}", static_cast<Float64>(std::numeric_limits<Float32>::max())));
    executeAndAssert<Float32>(
        func_name,
        std::numeric_limits<Float32>::min(),
        fmt::format("{}", static_cast<Float64>(std::numeric_limits<Float32>::min())));
    executeAndAssert<Float32>(
        func_name,
        std::numeric_limits<Float32>::lowest(),
        fmt::format("{}", static_cast<Float64>(std::numeric_limits<Float32>::lowest())));

    /// Float64
    executeAndAssert<Float64>(func_name, 0, "0");
    executeAndAssert<Float64>(func_name, 999.999, "999.999");
    executeAndAssert<Float64>(func_name, -999.999, "-999.999");
    executeAndAssert<Float64>(
        func_name,
        std::numeric_limits<Float64>::max(),
        fmt::format("{}", std::numeric_limits<Float64>::max()));
    executeAndAssert<Float64>(
        func_name,
        std::numeric_limits<Float64>::min(),
        fmt::format("{}", std::numeric_limits<Float64>::min()));
    executeAndAssert<Float64>(
        func_name,
        std::numeric_limits<Float64>::lowest(),
        fmt::format("{}", std::numeric_limits<Float64>::lowest()));
}
CATCH

TEST_F(TestCastAsJson, CastDecimalAsJson)
try
{
    const String func_name = "cast_decimal_as_json";

    using DecimalField32 = DecimalField<Decimal32>;
    using DecimalField64 = DecimalField<Decimal64>;
    using DecimalField128 = DecimalField<Decimal128>;
    using DecimalField256 = DecimalField<Decimal256>;

    /// Decimal32
    executeAndAssert(func_name, DecimalField32(1011, 1), "101.1");
    executeAndAssert(func_name, DecimalField32(-1011, 1), "-101.1");
    executeAndAssert(func_name, DecimalField32(9999, 1), "999.9");
    executeAndAssert(func_name, DecimalField32(-9999, 1), "-999.9");

    /// Decimal64
    executeAndAssert(func_name, DecimalField64(1011, 1), "101.1");
    executeAndAssert(func_name, DecimalField64(-1011, 1), "-101.1");
    executeAndAssert(func_name, DecimalField64(9999, 1), "999.9");
    executeAndAssert(func_name, DecimalField64(-9999, 1), "-999.9");

    /// Decimal128
    executeAndAssert(func_name, DecimalField128(1011, 1), "101.1");
    executeAndAssert(func_name, DecimalField128(-1011, 1), "-101.1");
    executeAndAssert(func_name, DecimalField128(9999, 1), "999.9");
    executeAndAssert(func_name, DecimalField128(-9999, 1), "-999.9");

    /// Decimal256
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(1011), 1), "101.1");
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(-1011), 1), "-101.1");
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(9999), 1), "999.9");
    executeAndAssert(func_name, DecimalField256(static_cast<Int256>(-9999), 1), "-999.9");
}
CATCH

TEST_F(TestCastAsJson, CastStringAsJson)
try
{
    // Only raw function test is tested, so output_tidb_tp is always nullptr and only the case of parsing json is tested here.
    // Because of `bool useDefaultImplementationForNulls() const override { return true; }`, null column is need to be tested here.

    const String func_name = "cast_string_as_json";

    /// case1 only null
    {
        ColumnWithTypeAndName only_null_const = createOnlyNullColumnConst(1);
        ColumnsWithTypeAndName input{only_null_const};
        auto res = executeFunction(func_name, input, nullptr, true);
        ASSERT_COLUMN_EQ(only_null_const, res);
    }

    /// case2 nullable column
    {
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({{}, "[]"});
        auto res = executeFunctionWithCast<true>(func_name, {nullable_column});
        ASSERT_COLUMN_EQ(nullable_column, res);
    }
    // invalid json text.
    {
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({""});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({"dsadhgashg"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }

    /// case3 not null
    // invalid json text
    {
        // empty document
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({""});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        // invaild json
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({"a"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        // invaild json
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>({"{fds, 1}"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    {
        // too deep
        ColumnWithTypeAndName nullable_column = createColumn<Nullable<String>>(
            {"[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
             "[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"
             "]]]]]]]]]]"});
        ASSERT_THROW(executeFunctionWithCast<true>(func_name, {nullable_column}), Exception);
    }
    // valid json text
    // a. literal
    executeAndAssert<String, true>(func_name, "0", "0");
    executeAndAssert<String, true>(func_name, "1", "1");
    executeAndAssert<String, true>(func_name, "-1", "-1");
    executeAndAssert<String, true>(func_name, "1.11", "1.11");
    executeAndAssert<String, true>(func_name, "-1.11", "-1.11");
    executeAndAssert<String, true>(func_name, "\"a\"", "\"a\"");
    executeAndAssert<String, true>(func_name, "true", "true");
    executeAndAssert<String, true>(func_name, "false", "false");
    executeAndAssert<String, true>(func_name, "null", "null");
    // b. json array
    executeAndAssert<String, true>(func_name, "[]", "[]");
    executeAndAssert<String, true>(func_name, "[1, 1000, 2.22, \"a\", null]", "[1, 1000, 2.22, \"a\", null]");
    executeAndAssert<String, true>(
        func_name,
        R"([1, 1000, 2.22, "a", null, {"a":1.11}])",
        R"([1, 1000, 2.22, "a", null, {"a": 1.11}])");
    executeAndAssert<String, true>(
        func_name,
        "[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]",
        "[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]");
    // c. json object
    executeAndAssert<String, true>(func_name, "{}", "{}");
    executeAndAssert<String, true>(func_name, "{\"a\":1}", "{\"a\": 1}");
    executeAndAssert<String, true>(
        func_name,
        R"({"a":null,"b":1,"c":1.11,"d":[],"e":{}})",
        R"({"a": null, "b": 1, "c": 1.11, "d": [], "e": {}})");
    executeAndAssert<String, true>(
        func_name,
        R"({"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{}}}}}}}}}}})",
        R"({"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {"a": {}}}}}}}}}}})");

    // duplicate keys
    // https://github.com/pingcap/tiflash/issues/8712
    executeAndAssert<String, true>(func_name, R"({"a":1, "a":2})", R"({"a": 2})");
    executeAndAssert<String, true>(func_name, R"({"a":2, "a":3, "a":1})", R"({"a": 1})");
    executeAndAssert<String, true>(func_name, R"({"a":1, "A":1})", R"({"A": 1, "a": 1})");
    String complex_json
        = R"({"粤U":100001,"鄂D":100002,"黑V":100003,"湘A":100004,"黑D":100005,"台M":100006,"浙T":100007,"新S":100008,"辽E":100009,"津W":100010,"台N":100011,"鄂Y":100012,"京M":100013,"黑W":100014,"赣V":100015,"甘J":100016,"辽Y":100017,"京G":100018,"湘P":100019,"台H":100020,"浙J":100021,"苏_":100022,"青W":100023,"澳E":100024,"台M":100025,"新K":100026,"琼H":100027,"粤Y":100028,"皖U":100029,"澳O":100030,"闽Y":100031,"甘Y":100032,"宁Q":100033,"甘V":100034,"浙T":100035,"新Q":100036,"澳Y":100037,"粤Z":100038,"甘W":100039,"蒙N":100040,"闽Z":100041,"吉L":100042,"黑Y":100043,"粤B":100044,"台X":100045,"湘V":100046,"豫M":100047,"青N":100048,"冀D":100049,"皖D":100050,"闽A":100051,"冀P":100052,"甘Z":100053,"渝S":100054,"渝N":100055,"渝A":100056,"琼I":100057,"澳W":100058,"皖_":100059,"台Z":100060,"川H":100061,"台A":100062,"云X":100063,"藏K":100064,"鄂I":100065,"豫W":100066,"闽Z":100067,"陕J":100068,"琼J":100069,"冀Q":100070,"蒙F":100071,"云K":100072,"苏U":100073,"吉A":100074,"豫S":100075,"津B":100076,"黑_":100077,"豫Z":100078,"豫_":100079,"苏C":100080,"云T":100081,"渝J":100082,"苏Y":100083,"皖W":100084,"皖_":100085,"台_":100086,"京B":100087,"湘F":100088,"云J":100089,"渝P":100090,"桂V":100091,"甘G":100092,"蒙G":100093,"苏I":100094,"湘O":100095,"藏M":100096,"琼C":100097,"藏T":100098,"黑B":100099,"晋R":100100,"甘U":100101,"云R":100102,"陕H":100103,"鄂Q":100104,"桂W":100105,"吉T":100106,"陕B":100107,"皖L":100108,"辽V":100109,"新M":100110,"豫K":100111,"鄂H":100112,"琼R":100113,"蒙_":100114,"豫Y":100115,"京K":100116,"港L":100117,"苏R":100118,"冀S":100119,"沪H":100120,"豫Y":100121,"冀_":100122,"赣H":100123,"渝S":100124,"津G":100125,"津O":100126,"赣T":100127,"湘F":100128,"云I":100129,"澳X":100130,"甘A":100131,"桂M":100132,"藏P":100133,"晋C":100134,"吉C":100135,"桂G":100136,"贵W":100137,"港G":100138,"浙L":100139,"青R":100140,"港S":100141,"陕C":100142,"陕Y":100143,"晋C":100144,"藏V":100145,"鄂C":100146,"台S":100147,"赣G":100148,"粤B":100149,"津B":100150,"渝L":100151,"川Y":100152,"澳D":100153,"贵G":100154,"澳E":100155,"港D":100156,"藏S":100157,"港R":100158,"鄂L":100159,"粤N":100160,"豫I":100161,"渝R":100162,"藏E":100163,"赣U":100164,"苏S":100165,"辽C":100166,"青Q":100167,"吉M":100168,"新M":100169,"黑U":100170,"豫N":100171,"港F":100172,"黑P":100173,"云G":100174,"甘B":100175,"贵J":100176,"蒙_":100177,"京F":100178,"台I":100179,"陕H":100180,"新A":100181,"津C":100182,"鲁N":100183,"苏N":100184,"鲁D":100185,"闽B":100186,"黑B":100187,"蒙T":100188,"琼J":100189,"京_":100190,"港Q":100191,"鲁X":100192,"鲁A":100193,"津H":100194,"豫A":100195,"吉F":100196,"沪S":100197,"云Y":100198,"津K":100199,"宁E":100200,"湘X":100201,"甘C":100202,"琼C":100203,"川X":100204,"港Y":100205,"宁P":100206,"辽K":100207,"黑B":100208,"晋S":100209,"贵P":100210,"粤A":100211,"青W":100212,"川T":100213,"冀N":100214,"沪W":100215,"津Q":100216,"台A":100217,"黑E":100218,"津S":100219,"鲁P":100220,"澳B":100221,"浙Z":100222,"桂S":100223,"宁H":100224,"藏E":100225,"鄂Z":100226,"黑J":100227,"津E":100228,"贵C":100229,"辽X":100230,"贵Z":100231,"吉K":100232,"津A":100233,"京_":100234,"川I":100235,"台A":100236,"云X":100237,"藏M":100238,"川U":100239,"鲁M":100240,"黑X":100241,"桂B":100242,"粤Q":100243,"粤X":100244,"辽L":100245,"桂H":100246,"川J":100247,"云Y":100248,"港Z":100249,"京D":100250,"吉Z":100251,"吉O":100252,"吉S":100253,"鄂J":100254,"皖O":100255,"吉U":100256,"贵L":100257,"港I":100258,"琼M":100259,"吉Y":100260,"甘_":100261,"新V":100262,"琼C":100263,"贵K":100264,"贵L":100265,"津X":100266,"黑R":100267,"赣I":100268,"辽F":100269,"鲁C":100270,"桂F":100271,"晋F":100272,"黑R":100273,"晋W":100274,"桂B":100275,"吉F":100276,"台J":100277,"粤U":100278,"甘L":100279,"桂I":100280,"澳Z":100281,"粤Z":100282,"浙H":100283,"冀X":100284,"台G":100285,"湘T":100286,"贵L":100287,"港J":100288,"沪H":100289,"渝U":100290,"桂I":100291,"京D":100292,"桂G":100293,"陕L":100294,"湘D":100295,"青O":100296,"苏I":100297,"新Y":100298,"鄂S":100299,"京A":100300,"台Y":100301,"陕J":100302,"澳R":100303,"浙F":100304,"吉S":100305,"浙E":100306,"赣U":100307,"鄂E":100308,"苏M":100309,"云M":100310,"川T":100311,"津D":100312,"晋K":100313,"黑C":100314,"贵I":100315,"苏A":100316,"澳R":100317,"苏C":100318,"豫Q":100319,"鄂Q":100320,"陕J":100321,"港M":100322,"蒙Q":100323,"湘N":100324,"贵C":100325,"皖H":100326,"苏F":100327,"青K":100328,"鄂H":100329,"港U":100330,"渝G":100331,"藏J":100332,"苏X":100333,"辽L":100334,"晋N":100335,"藏O":100336,"湘T":100337,"青T":100338,"皖I":100339,"闽O":100340,"晋G":100341,"甘A":100342,"苏Z":100343,"晋_":100344,"云_":100345,"新Y":100346,"鄂T":100347,"沪F":100348,"粤F":100349,"闽Y":100350,"鄂T":100351,"沪H":100352,"冀X":100353,"京J":100354,"粤S":100355,"渝P":100356,"鄂T":100357,"豫S":100358,"澳R":100359,"赣I":100360,"渝S":100361,"冀P":100362,"鄂R":100363,"宁J":100364,"黑C":100365,"闽G":100366,"冀X":100367,"沪R":100368,"津H":100369,"黑L":100370,"苏R":100371,"沪M":100372,"沪_":100373,"陕F":100374,"辽L":100375,"赣_":100376,"晋Z":100377,"澳Z":100378,"琼R":100379,"晋Y":100380,"贵_":100381,"青X":100382,"津O":100383,"陕O":100384,"藏M":100385,"川V":100386,"桂D":100387,"津Y":100388,"鲁B":100389,"云V":100390,"鄂B":100391,"粤R":100392,"京B":100393,"湘H":100394,"赣O":100395,"湘V":100396,"渝C":100397,"黑Y":100398,"藏I":100399,"渝P":100400,"甘_":100401,"辽H":100402,"陕I":100403,"粤V":100404,"吉Y":100405,"陕K":100406,"津A":100407,"台Y":100408,"甘A":100409,"新X":100410,"辽O":100411,"津O":100412,"陕N":100413,"晋J":100414,"京C":100415,"鲁M":100416,"鲁J":100417,"蒙B":100418,"新V":100419,"粤L":100420,"港I":100421,"贵S":100422,"鲁R":100423,"冀V":100424,"甘K":100425,"湘E":100426,"渝Y":100427,"皖T":100428,"藏U":100429,"鄂B":100430,"琼G":100431,"蒙H":100432,"蒙D":100433,"青O":100434,"浙O":100435,"新F":100436,"澳E":100437,"台J":100438,"藏B":100439,"粤S":100440,"沪K":100441,"粤N":100442,"黑T":100443,"宁D":100444,"冀G":100445,"藏I":100446,"云Z":100447,"豫_":100448,"鲁W":100449,"豫I":100450,"京D":100451,"蒙R":100452,"新Z":100453,"晋B":100454,"晋T":100455,"澳N":100456,"贵B":100457,"云S":100458,"宁I":100459,"琼P":100460,"豫D":100461,"冀G":100462,"川L":100463,"吉B":100464,"辽Y":100465,"港W":100466,"鄂Y":100467,"冀Y":100468,"沪S":100469,"豫V":100470,"浙W":100471,"港B":100472,"赣D":100473,"黑X":100474,"蒙J":100475,"京A":100476,"京C":100477,"苏S":100478,"湘G":100479,"鄂Q":100480,"闽O":100481,"晋J":100482,"京B":100483,"湘E":100484,"京P":100485,"川K":100486,"甘O":100487,"渝V":100488,"贵H":100489,"渝T":100490,"皖L":100491,"冀M":100492,"琼A":100493,"桂N":100494,"京M":100495,"皖E":100496,"川U":100497,"皖F":100498,"渝X":100499,"京M":100500,"鲁H":100501,"鲁Y":100502,"甘_":100503,"湘L":100504,"辽Y":100505,"宁Q":100506,"浙K":100507,"甘R":100508,"桂S":100509,"宁I":100510,"港S":100511,"吉G":100512,"云F":100513,"鄂V":100514,"桂G":100515,"陕M":100516,"新N":100517,"鲁F":100518,"台K":100519,"港O":100520,"藏M":100521,"川V":100522,"桂E":100523,"皖C":100524,"鄂D":100525,"黑W":100526,"鄂W":100527,"青L":100528,"粤N":100529,"湘Y":100530,"吉R":100531,"辽E":100532,"云M":100533,"粤K":100534,"吉C":100535,"甘L":100536,"鄂F":100537,"甘C":100538,"藏V":100539,"甘H":100540,"宁K":100541,"浙_":100542,"甘X":100543,"闽V":100544,"台O":100545,"粤E":100546,"晋H":100547,"粤Z":100548,"新A":100549,"渝J":100550,"浙D":100551,"新M":100552,"辽S":100553,"藏A":100554,"晋Y":100555,"青F":100556,"吉U":100557,"贵L":100558,"宁D":100559,"台N":100560,"蒙N":100561,"陕R":100562,"浙C":100563,"黑A":100564,"湘K":100565,"京Z":100566,"吉N":100567,"赣X":100568,"台H":100569,"鄂O":100570,"黑Q":100571,"湘P":100572,"沪P":100573,"吉O":100574,"藏K":100575,"晋S":100576,"贵N":100577,"新J":100578,"晋W":100579,"苏P":100580,"陕L":100581,"云T":100582,"台U":100583,"川Z":100584,"云E":100585,"黑V":100586,"冀K":100587,"浙A":100588,"粤S":100589,"豫W":100590,"蒙O":100591,"宁S":100592,"琼K":100593,"黑C":100594,"粤M":100595,"京R":100596,"黑H":100597,"蒙E":100598,"津U":100599,"闽D":100600,"晋N":100601,"贵E":100602,"晋M":100603,"蒙P":100604,"津P":100605,"闽U":100606,"青P":100607,"桂W":100608,"吉V":100609,"京R":100610,"云_":100611,"鲁V":100612,"澳L":100613,"云T":100614,"京Y":100615,"浙N":100616,"黑R":100617,"甘T":100618,"京Y":100619,"浙N":100620,"辽Q":100621,"浙F":100622,"川O":100623,"黑P":100624,"云G":100625,"桂Z":100626,"辽F":100627,"苏K":100628,"粤O":100629,"赣R":100630,"琼N":100631,"藏O":100632,"皖Y":100633,"甘C":100634,"藏T":100635,"辽Z":100636,"京F":100637,"京L":100638,"豫M":100639,"藏R":100640,"宁K":100641,"鲁N":100642,"桂B":100643,"粤O":100644,"浙O":100645,"浙O":100646,"鲁E":100647,"粤I":100648,"皖Q":100649,"沪O":100650,"鄂V":100651,"鄂A":100652,"吉H":100653,"浙F":100654,"川N":100655,"渝Y":100656,"皖V":100657,"台P":100658,"澳A":100659,"云Z":100660,"渝U":100661,"苏U":100662,"蒙S":100663,"蒙_":100664,"渝P":100665,"蒙I":100666,"闽P":100667,"闽V":100668,"台M":100669,"苏P":100670,"闽S":100671,"新V":100672,"宁Z":100673,"渝R":100674,"琼R":100675,"鄂M":100676,"川X":100677,"宁S":100678,"川B":100679,"琼F":100680,"苏J":100681,"桂N":100682,"台K":100683,"京Z":100684,"陕I":100685,"粤V":100686,"闽B":100687,"辽_":100688,"云A":100689,"桂O":100690,"沪U":100691,"甘M":100692,"陕V":100693,"津X":100694,"湘U":100695,"澳O":100696,"贵B":100697,"京A":100698,"沪H":100699,"豫Z":100700,"黑K":100701,"辽_":100702,"黑H":100703,"蒙F":100704,"黑P":100705,"豫C":100706,"京V":100707,"宁Z":100708,"云B":100709,"闽H":100710,"皖S":100711,"晋U":100712,"冀K":100713,"甘P":100714,"云H":100715,"青D":100716,"湘Z":100717,"澳Y":100718,"陕N":100719,"蒙O":100720,"琼W":100721,"甘G":100722,"赣Q":100723,"粤X":100724,"沪V":100725,"青M":100726,"台O":100727,"闽W":100728,"豫I":100729,"沪L":100730,"澳M":100731,"鄂B":100732,"宁C":100733,"港I":100734,"宁H":100735,"琼Q":100736,"鲁X":100737,"苏H":100738,"黑L":100739,"苏R":100740,"津H":100741,"黑M":100742,"甘K":100743,"黑D":100744,"川X":100745,"粤E":100746,"吉V":100747,"澳J":100748,"港Q":100749,"鲁U":100750,"川W":100751,"贵B":100752,"冀L":100753,"藏V":100754,"晋M":100755,"晋N":100756,"藏O":100757,"辽O":100758,"台J":100759,"贵P":100760,"贵Y":100761,"苏F":100762,"青H":100763,"豫Z":100764,"冀Z":100765,"皖S":100766,"晋U":100767,"渝H":100768,"沪M":100769,"渝D":100770,"苏Q":100771,"宁M":100772,"贵F":100773,"宁T":100774,"澳S":100775,"桂P":100776,"黑Q":100777,"湘N":100778,"闽_":100779,"琼M":100780,"贵C":100781,"新O":100782,"陕N":100783,"晋M":100784,"鄂B":100785,"川A":100786,"晋U":100787,"云Q":100788,"赣L":100789,"青Q":100790,"贵W":100791,"台K":100792,"台U":100793,"港H":100794,"闽Q":100795,"津H":100796,"新Z":100797,"蒙G":100798,"鲁_":100799,"澳W":100800,"湘W":100801,"赣S":100802,"京C":100803,"赣Y":100804,"辽I":100805,"京D":100806,"晋N":100807,"贵C":100808,"皖G":100809,"鲁Z":100810,"闽P":100811,"宁P":100812,"皖W":100813,"湘V":100814,"黑V":100815,"辽T":100816,"澳Q":100817,"云E":100818,"黑T":100819,"琼J":100820,"京C":100821,"鲁L":100822,"津A":100823,"港P":100824,"豫B":100825,"藏V":100826,"陕V":100827,"冀E":100828,"甘D":100829,"冀E":100830,"苏O":100831,"赣R":100832,"港Q":100833,"苏C":100834,"湘E":100835,"港D":100836,"宁A":100837,"苏A":100838,"藏B":100839,"川A":100840,"蒙_":100841,"渝P":100842,"蒙F":100843,"皖W":100844,"皖B":100845,"皖K":100846,"沪E":100847,"蒙P":100848,"津O":100849,"赣T":100850,"黑B":100851,"鄂J":100852,"湘I":100853,"贵R":100854,"豫X":100855,"青K":100856,"甘O":100857,"冀_":100858,"甘V":100859,"苏P":100860,"吉P":100861,"川N":100862,"港D":100863,"台M":100864,"皖E":100865,"宁W":100866,"蒙O":100867,"琼Y":100868,"青G":100869,"琼S":100870,"青X":100871,"津P":100872,"闽U":100873,"贵H":100874,"皖T":100875,"晋V":100876,"辽S":100877,"宁G":100878,"甘A":100879,"桂N":100880,"京N":100881,"浙R":100882,"港U":100883,"港L":100884,"湘F":100885,"豫E":100886,"新H":100887,"苏C":100888,"辽W":100889,"甘F":100890,"苏J":100891,"晋V":100892,"皖C":100893,"鄂G":100894,"青D":100895,"湘_":100896,"沪M":100897,"京T":100898,"蒙X":100899,"鄂X":100900,"川R":100901,"青W":100902,"宁V":100903,"苏Q":100904,"宁N":100905,"川U":100906,"湘D":100907,"川T":100908,"冀O":100909,"湘W":100910,"湘Y":100911,"吉Q":100912,"冀W":100913,"青I":100914,"皖R":100915,"云B":100916,"贵L":100917,"京T":100918,"吉D":100919,"闽C":100920,"鲁M":100921,"云R":100922,"晋A":100923,"新U":100924,"吉B":100925,"鲁O":100926,"蒙L":100927,"鲁J":100928,"粤U":100929,"晋N":100930,"川Q":100931,"吉L":100932,"渝F":100933,"藏K":100934,"桂J":100935,"黑F":100936,"云L":100937,"甘L":100938,"赣_":100939,"陕G":100940,"黑O":100941,"台G":100942,"湘T":100943,"粤P":100944,"晋D":100945,"台N":100946,"晋K":100947,"豫T":100948,"渝K":100949,"甘P":100950,"皖U":100951,"宁D":100952,"港J":100953,"台V":100954,"渝D":100955,"黑_":100956,"津J":100957,"甘S":100958,"川B":100959,"宁E":100960,"黑S":100961,"青U":100962,"苏V":100963,"川U":100964,"湘C":100965,"贵H":100966,"云C":100967,"澳N":100968,"吉U":100969,"琼C":100970,"宁_":100971,"鲁V":100972,"澳K":100973,"台X":100974,"皖Z":100975,"藏E":100976,"桂A":100977,"桂N":100978,"琼C":100979,"粤M":100980,"京U":100981,"青Q":100982,"粤X":100983,"津R":100984,"冀T":100985,"豫R":100986,"青_":100987,"蒙A":100988,"新T":100989,"赣C":100990,"冀K":100991,"甘O":100992,"沪T":100993,"鄂F":100994,"陕M":100995,"浙T":100996,"湘H":100997,"甘Y":100998,"港W":100999,"浙S":101000})";
    String complex_str
        = R"({"云A": 100689, "云B": 100916, "云C": 100967, "云E": 100818, "云F": 100513, "云G": 100625, "云H": 100715, "云I": 100129, "云J": 100089, "云K": 100072, "云L": 100937, "云M": 100533, "云Q": 100788, "云R": 100922, "云S": 100458, "云T": 100614, "云V": 100390, "云X": 100237, "云Y": 100248, "云Z": 100660, "云_": 100611, "京A": 100698, "京B": 100483, "京C": 100821, "京D": 100806, "京F": 100637, "京G": 100018, "京J": 100354, "京K": 100116, "京L": 100638, "京M": 100500, "京N": 100881, "京P": 100485, "京R": 100610, "京T": 100918, "京U": 100981, "京V": 100707, "京Y": 100619, "京Z": 100684, "京_": 100234, "冀D": 100049, "冀E": 100830, "冀G": 100462, "冀K": 100991, "冀L": 100753, "冀M": 100492, "冀N": 100214, "冀O": 100909, "冀P": 100362, "冀Q": 100070, "冀S": 100119, "冀T": 100985, "冀V": 100424, "冀W": 100913, "冀X": 100367, "冀Y": 100468, "冀Z": 100765, "冀_": 100858, "台A": 100236, "台G": 100942, "台H": 100569, "台I": 100179, "台J": 100759, "台K": 100792, "台M": 100864, "台N": 100946, "台O": 100727, "台P": 100658, "台S": 100147, "台U": 100793, "台V": 100954, "台X": 100974, "台Y": 100408, "台Z": 100060, "台_": 100086, "吉A": 100074, "吉B": 100925, "吉C": 100535, "吉D": 100919, "吉F": 100276, "吉G": 100512, "吉H": 100653, "吉K": 100232, "吉L": 100932, "吉M": 100168, "吉N": 100567, "吉O": 100574, "吉P": 100861, "吉Q": 100912, "吉R": 100531, "吉S": 100305, "吉T": 100106, "吉U": 100969, "吉V": 100747, "吉Y": 100405, "吉Z": 100251, "宁A": 100837, "宁C": 100733, "宁D": 100952, "宁E": 100960, "宁G": 100878, "宁H": 100735, "宁I": 100510, "宁J": 100364, "宁K": 100641, "宁M": 100772, "宁N": 100905, "宁P": 100812, "宁Q": 100506, "宁S": 100678, "宁T": 100774, "宁V": 100903, "宁W": 100866, "宁Z": 100708, "宁_": 100971, "川A": 100840, "川B": 100959, "川H": 100061, "川I": 100235, "川J": 100247, "川K": 100486, "川L": 100463, "川N": 100862, "川O": 100623, "川Q": 100931, "川R": 100901, "川T": 100908, "川U": 100964, "川V": 100522, "川W": 100751, "川X": 100745, "川Y": 100152, "川Z": 100584, "新A": 100549, "新F": 100436, "新H": 100887, "新J": 100578, "新K": 100026, "新M": 100552, "新N": 100517, "新O": 100782, "新Q": 100036, "新S": 100008, "新T": 100989, "新U": 100924, "新V": 100672, "新X": 100410, "新Y": 100346, "新Z": 100797, "晋A": 100923, "晋B": 100454, "晋C": 100144, "晋D": 100945, "晋F": 100272, "晋G": 100341, "晋H": 100547, "晋J": 100482, "晋K": 100947, "晋M": 100784, "晋N": 100930, "晋R": 100100, "晋S": 100576, "晋T": 100455, "晋U": 100787, "晋V": 100892, "晋W": 100579, "晋Y": 100555, "晋Z": 100377, "晋_": 100344, "桂A": 100977, "桂B": 100643, "桂D": 100387, "桂E": 100523, "桂F": 100271, "桂G": 100515, "桂H": 100246, "桂I": 100291, "桂J": 100935, "桂M": 100132, "桂N": 100978, "桂O": 100690, "桂P": 100776, "桂S": 100509, "桂V": 100091, "桂W": 100608, "桂Z": 100626, "沪E": 100847, "沪F": 100348, "沪H": 100699, "沪K": 100441, "沪L": 100730, "沪M": 100897, "沪O": 100650, "沪P": 100573, "沪R": 100368, "沪S": 100469, "沪T": 100993, "沪U": 100691, "沪V": 100725, "沪W": 100215, "沪_": 100373, "津A": 100823, "津B": 100150, "津C": 100182, "津D": 100312, "津E": 100228, "津G": 100125, "津H": 100796, "津J": 100957, "津K": 100199, "津O": 100849, "津P": 100872, "津Q": 100216, "津R": 100984, "津S": 100219, "津U": 100599, "津W": 100010, "津X": 100694, "津Y": 100388, "浙A": 100588, "浙C": 100563, "浙D": 100551, "浙E": 100306, "浙F": 100654, "浙H": 100283, "浙J": 100021, "浙K": 100507, "浙L": 100139, "浙N": 100620, "浙O": 100646, "浙R": 100882, "浙S": 101000, "浙T": 100996, "浙W": 100471, "浙Z": 100222, "浙_": 100542, "渝A": 100056, "渝C": 100397, "渝D": 100955, "渝F": 100933, "渝G": 100331, "渝H": 100768, "渝J": 100550, "渝K": 100949, "渝L": 100151, "渝N": 100055, "渝P": 100842, "渝R": 100674, "渝S": 100361, "渝T": 100490, "渝U": 100661, "渝V": 100488, "渝X": 100499, "渝Y": 100656, "港B": 100472, "港D": 100863, "港F": 100172, "港G": 100138, "港H": 100794, "港I": 100734, "港J": 100953, "港L": 100884, "港M": 100322, "港O": 100520, "港P": 100824, "港Q": 100833, "港R": 100158, "港S": 100511, "港U": 100883, "港W": 100999, "港Y": 100205, "港Z": 100249, "湘A": 100004, "湘C": 100965, "湘D": 100907, "湘E": 100835, "湘F": 100885, "湘G": 100479, "湘H": 100997, "湘I": 100853, "湘K": 100565, "湘L": 100504, "湘N": 100778, "湘O": 100095, "湘P": 100572, "湘T": 100943, "湘U": 100695, "湘V": 100814, "湘W": 100910, "湘X": 100201, "湘Y": 100911, "湘Z": 100717, "湘_": 100896, "澳A": 100659, "澳B": 100221, "澳D": 100153, "澳E": 100437, "澳J": 100748, "澳K": 100973, "澳L": 100613, "澳M": 100731, "澳N": 100968, "澳O": 100696, "澳Q": 100817, "澳R": 100359, "澳S": 100775, "澳W": 100800, "澳X": 100130, "澳Y": 100718, "澳Z": 100378, "琼A": 100493, "琼C": 100979, "琼F": 100680, "琼G": 100431, "琼H": 100027, "琼I": 100057, "琼J": 100820, "琼K": 100593, "琼M": 100780, "琼N": 100631, "琼P": 100460, "琼Q": 100736, "琼R": 100675, "琼S": 100870, "琼W": 100721, "琼Y": 100868, "甘A": 100879, "甘B": 100175, "甘C": 100634, "甘D": 100829, "甘F": 100890, "甘G": 100722, "甘H": 100540, "甘J": 100016, "甘K": 100743, "甘L": 100938, "甘M": 100692, "甘O": 100992, "甘P": 100950, "甘R": 100508, "甘S": 100958, "甘T": 100618, "甘U": 100101, "甘V": 100859, "甘W": 100039, "甘X": 100543, "甘Y": 100998, "甘Z": 100053, "甘_": 100503, "皖B": 100845, "皖C": 100893, "皖D": 100050, "皖E": 100865, "皖F": 100498, "皖G": 100809, "皖H": 100326, "皖I": 100339, "皖K": 100846, "皖L": 100491, "皖O": 100255, "皖Q": 100649, "皖R": 100915, "皖S": 100766, "皖T": 100875, "皖U": 100951, "皖V": 100657, "皖W": 100844, "皖Y": 100633, "皖Z": 100975, "皖_": 100085, "粤A": 100211, "粤B": 100149, "粤E": 100746, "粤F": 100349, "粤I": 100648, "粤K": 100534, "粤L": 100420, "粤M": 100980, "粤N": 100529, "粤O": 100644, "粤P": 100944, "粤Q": 100243, "粤R": 100392, "粤S": 100589, "粤U": 100929, "粤V": 100686, "粤X": 100983, "粤Y": 100028, "粤Z": 100548, "苏A": 100838, "苏C": 100888, "苏F": 100762, "苏H": 100738, "苏I": 100297, "苏J": 100891, "苏K": 100628, "苏M": 100309, "苏N": 100184, "苏O": 100831, "苏P": 100860, "苏Q": 100904, "苏R": 100740, "苏S": 100478, "苏U": 100662, "苏V": 100963, "苏X": 100333, "苏Y": 100083, "苏Z": 100343, "苏_": 100022, "蒙A": 100988, "蒙B": 100418, "蒙D": 100433, "蒙E": 100598, "蒙F": 100843, "蒙G": 100798, "蒙H": 100432, "蒙I": 100666, "蒙J": 100475, "蒙L": 100927, "蒙N": 100561, "蒙O": 100867, "蒙P": 100848, "蒙Q": 100323, "蒙R": 100452, "蒙S": 100663, "蒙T": 100188, "蒙X": 100899, "蒙_": 100841, "藏A": 100554, "藏B": 100839, "藏E": 100976, "藏I": 100446, "藏J": 100332, "藏K": 100934, "藏M": 100521, "藏O": 100757, "藏P": 100133, "藏R": 100640, "藏S": 100157, "藏T": 100635, "藏U": 100429, "藏V": 100826, "豫A": 100195, "豫B": 100825, "豫C": 100706, "豫D": 100461, "豫E": 100886, "豫I": 100729, "豫K": 100111, "豫M": 100639, "豫N": 100171, "豫Q": 100319, "豫R": 100986, "豫S": 100358, "豫T": 100948, "豫V": 100470, "豫W": 100590, "豫X": 100855, "豫Y": 100121, "豫Z": 100764, "豫_": 100448, "贵B": 100752, "贵C": 100808, "贵E": 100602, "贵F": 100773, "贵G": 100154, "贵H": 100966, "贵I": 100315, "贵J": 100176, "贵K": 100264, "贵L": 100917, "贵N": 100577, "贵P": 100760, "贵R": 100854, "贵S": 100422, "贵W": 100791, "贵Y": 100761, "贵Z": 100231, "贵_": 100381, "赣C": 100990, "赣D": 100473, "赣G": 100148, "赣H": 100123, "赣I": 100360, "赣L": 100789, "赣O": 100395, "赣Q": 100723, "赣R": 100832, "赣S": 100802, "赣T": 100850, "赣U": 100307, "赣V": 100015, "赣X": 100568, "赣Y": 100804, "赣_": 100939, "辽C": 100166, "辽E": 100532, "辽F": 100627, "辽H": 100402, "辽I": 100805, "辽K": 100207, "辽L": 100375, "辽O": 100758, "辽Q": 100621, "辽S": 100877, "辽T": 100816, "辽V": 100109, "辽W": 100889, "辽X": 100230, "辽Y": 100505, "辽Z": 100636, "辽_": 100702, "鄂A": 100652, "鄂B": 100785, "鄂C": 100146, "鄂D": 100525, "鄂E": 100308, "鄂F": 100994, "鄂G": 100894, "鄂H": 100329, "鄂I": 100065, "鄂J": 100852, "鄂L": 100159, "鄂M": 100676, "鄂O": 100570, "鄂Q": 100480, "鄂R": 100363, "鄂S": 100299, "鄂T": 100357, "鄂V": 100651, "鄂W": 100527, "鄂X": 100900, "鄂Y": 100467, "鄂Z": 100226, "闽A": 100051, "闽B": 100687, "闽C": 100920, "闽D": 100600, "闽G": 100366, "闽H": 100710, "闽O": 100481, "闽P": 100811, "闽Q": 100795, "闽S": 100671, "闽U": 100873, "闽V": 100668, "闽W": 100728, "闽Y": 100350, "闽Z": 100067, "闽_": 100779, "陕B": 100107, "陕C": 100142, "陕F": 100374, "陕G": 100940, "陕H": 100180, "陕I": 100685, "陕J": 100321, "陕K": 100406, "陕L": 100581, "陕M": 100995, "陕N": 100783, "陕O": 100384, "陕R": 100562, "陕V": 100827, "陕Y": 100143, "青D": 100895, "青F": 100556, "青G": 100869, "青H": 100763, "青I": 100914, "青K": 100856, "青L": 100528, "青M": 100726, "青N": 100048, "青O": 100434, "青P": 100607, "青Q": 100982, "青R": 100140, "青T": 100338, "青U": 100962, "青W": 100902, "青X": 100871, "青_": 100987, "鲁A": 100193, "鲁B": 100389, "鲁C": 100270, "鲁D": 100185, "鲁E": 100647, "鲁F": 100518, "鲁H": 100501, "鲁J": 100928, "鲁L": 100822, "鲁M": 100921, "鲁N": 100642, "鲁O": 100926, "鲁P": 100220, "鲁R": 100423, "鲁U": 100750, "鲁V": 100972, "鲁W": 100449, "鲁X": 100737, "鲁Y": 100502, "鲁Z": 100810, "鲁_": 100799, "黑A": 100564, "黑B": 100851, "黑C": 100594, "黑D": 100744, "黑E": 100218, "黑F": 100936, "黑H": 100703, "黑J": 100227, "黑K": 100701, "黑L": 100739, "黑M": 100742, "黑O": 100941, "黑P": 100705, "黑Q": 100777, "黑R": 100617, "黑S": 100961, "黑T": 100819, "黑U": 100170, "黑V": 100815, "黑W": 100526, "黑X": 100474, "黑Y": 100398, "黑_": 100956})";
    executeAndAssert<String, true>(func_name, complex_json, complex_str);
}
CATCH

TEST_F(TestCastAsJson, CastTimeAsJson)
try
{
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();

    // DataTypeMyDateTime
    // Only raw function test is tested, so input_tidb_tp is always nullptr and only the case of TiDB::TypeTimestamp is tested here.
    {
        auto data_col_ptr
            = createColumn<DataTypeMyDateTime::FieldType>(
                  {MyDateTime(2023, 1, 2, 3, 4, 5, 6).toPackedUInt(), MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt()})
                  .column;
        ColumnWithTypeAndName input(data_col_ptr, datetime_type_ptr, "");
        auto res = executeFunctionWithCast<true>("cast_time_as_json", {input});
        auto expect
            = createColumn<Nullable<String>>({"\"2023-01-02 03:04:05.000006\"", "\"0000-00-00 00:00:00.000000\""});
        ASSERT_COLUMN_EQ(expect, res);
    }

    // DataTypeMyDate
    {
        auto data_col_ptr = createColumn<DataTypeMyDate::FieldType>(
                                {MyDate(2023, 12, 31).toPackedUInt(), MyDate(0, 0, 0).toPackedUInt()})
                                .column;
        ColumnWithTypeAndName input(data_col_ptr, date_type_ptr, "");
        auto res = executeFunctionWithCast("cast_time_as_json", {input});
        auto expect = createColumn<Nullable<String>>({"\"2023-12-31\"", "\"0000-00-00\""});
        ASSERT_COLUMN_EQ(expect, res);
    }
}
CATCH

TEST_F(TestCastAsJson, CastDurationAsJson)
try
{
    {
        ColumnWithTypeAndName input(
            // 22hour, 22min, 22s, 222ms
            createColumn<DataTypeMyDuration::FieldType>({(22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L,
                                                         -1 * (22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L})
                .column,
            std::make_shared<DataTypeMyDuration>(6),
            "");

        auto res = executeFunctionWithCast("cast_duration_as_json", {input});
        auto expect = createColumn<Nullable<String>>({"\"22:22:22.222000\"", "\"-22:22:22.222000\""});
        ASSERT_COLUMN_EQ(expect, res);
    }
    {
        ColumnWithTypeAndName input(
            // 22hour, 22min, 22s, 222ms
            createColumn<DataTypeMyDuration::FieldType>({(22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L,
                                                         -1 * (22 * 3600000 + 22 * 60000 + 22 * 1000 + 222) * 1000000L})
                .column,
            std::make_shared<DataTypeMyDuration>(1),
            "");

        auto res = executeFunctionWithCast("cast_duration_as_json", {input});
        auto expect = createColumn<Nullable<String>>({"\"22:22:22.222000\"", "\"-22:22:22.222000\""});
        ASSERT_COLUMN_EQ(expect, res);
    }
}
CATCH

} // namespace DB::tests
