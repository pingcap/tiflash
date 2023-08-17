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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeNothing.h>
#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{
namespace tests
{
class TestFunctionBitAnd : public DB::tests::FunctionTest
{
};

#define ASSERT_BITAND(t1, t2, result) ASSERT_COLUMN_EQ(result, executeFunction("bitAnd", {t1, t2}))

TEST_F(TestFunctionBitAnd, Simple)
try
{
    ASSERT_BITAND(
        createColumn<Nullable<Int64>>({-1, 1}),
        createColumn<Nullable<Int64>>({0, 0}),
        createColumn<Nullable<UInt64>>({0, 0}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitAnd, others will be casted by TiDB Planner.
TEST_F(TestFunctionBitAnd, TypePromotion)
try
{
    // Type Promotion
    ASSERT_BITAND(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<Int32>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<Int32>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt16>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<UInt16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({0}));

    // Type Promotion across signed/unsigned
    ASSERT_BITAND(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<Int64>>({1}),
        createColumn<Nullable<UInt8>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({0}));
}
CATCH

TEST_F(TestFunctionBitAnd, Nullable)
try
{
    // Non Nullable
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Int16>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Int32>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<Int32>({1}), createColumn<Int64>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({0}));

    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<UInt16>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<UInt16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({0}));

    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<Int64>({1}), createColumn<UInt8>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Int16>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({0}));

    // Across Nullable and non-Nullable
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Nullable<Int32>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Int32>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<UInt16>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<UInt16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Int64>({1}), createColumn<Nullable<UInt8>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Int16>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<Int32>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int32>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<UInt16>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int64>>({1}), createColumn<UInt8>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<Int16>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({0}));
}
CATCH

TEST_F(TestFunctionBitAnd, TypeCastWithConst)
try
{
    /// need test these kinds of columns:
    /// 1. ColumnVector
    /// 2. ColumnVector<Nullable>
    /// 3. ColumnConst
    /// 4. ColumnConst<Nullable>, value != null
    /// 5. ColumnConst<Nullable>, value = null

    ASSERT_BITAND(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 0, 0, 1}));
    ASSERT_BITAND(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<UInt64>(4, 0),
        createColumn<UInt64>({0, 0, 0, 0}));
    ASSERT_BITAND(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createColumn<UInt64>({0, 0, 0, 0}));
    ASSERT_BITAND(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt)); // become const in wrapInNullable

    ASSERT_BITAND(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITAND(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITAND(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITAND(
        createConstColumn<Int8>(4, 0),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 0, 0, 0}));
    ASSERT_BITAND(
        createConstColumn<Int8>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createConstColumn<UInt64>(4, 0), createConstColumn<UInt64>(4, 0));
    ASSERT_BITAND(
        createConstColumn<Int8>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITAND(
        createConstColumn<Int8>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, 0),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 0, 0, 0}));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<UInt64>({0, 1, 0, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITAND(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitAnd, Boundary)
try
{
    ASSERT_BITAND(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt8>({0, 255, 0, 255}),
        createColumn<UInt64>({0, 127, 0, 128}));
    ASSERT_BITAND(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt16>({0, 65535, 0, 65535}),
        createColumn<UInt64>({0, 127, 0, 65408}));
    ASSERT_BITAND(
        createColumn<Int16>({32767, 32767, -32768, -32768}),
        createColumn<UInt8>({0, 255, 0, 255}),
        createColumn<UInt64>({0, 255, 0, 0}));

    ASSERT_BITAND(
        createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, 0, 0, 1, 0, UINT64_MAX, 0, INT64_MAX, 0, 9223372036854775808ull}));
}
CATCH

TEST_F(TestFunctionBitAnd, UINT64)
try
{
    ASSERT_BITAND(
        createColumn<UInt64>({0, 0, UINT64_MAX, UINT64_MAX}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, 0, 0, UINT64_MAX}));

    ASSERT_BITAND(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, 0, UINT64_MAX, std::nullopt, 0}),
        createColumn<Nullable<UInt64>>({0, 0, 0, UINT64_MAX, std::nullopt, std::nullopt}));

    ASSERT_BITAND(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, 0, 0, UINT64_MAX, std::nullopt}));

    ASSERT_BITAND(
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, 0, UINT64_MAX, std::nullopt}));

    /*
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<unsigned long long> dis(
        std::numeric_limits<std::uint64_t>::min(),
        std::numeric_limits<std::uint64_t>::max()
    );
    size_t count = 100;
    std::vector<UINT64> v1(count), v2(count), res(count);
    for (size_t i=0; i<count; ++i) {
        v1[i] = dis(gen);
        v2[i] = dis(gen);
        res[i] = v1[i] & v2[i];
    }
    */
    // clang-format off
    ASSERT_BITAND(createColumn<UInt64>({7091597907609164394ull,12147405979737555885ull,4944752083022751199ull,5266856817029714805ull,16249582031829054894ull,14585895400450077565ull,16559878625296112963ull,11686022872732312883ull,2252836050652542276ull,18270461639320085260ull,477362305064009683ull,5924031996839311984ull,13502342125078821090ull,1983692735111761557ull,7075393861938658224ull,8534577556983106270ull,6961865328371185981ull,8145880463914069438ull,5244290579560821188ull,10259565555661135100ull,4653092958722629712ull,8153941146514590265ull,16445187578470766485ull,126971754730186422ull,12494401415606381041ull,821635861271395080ull,10789756166060569460ull,17220753103104465161ull,4214870374383746276ull,11087492977524287663ull,13202884495831508537ull,15975448051191870337ull,14627676554537635677ull,4349811632778009896ull,17130992699769672908ull,11200975303296257603ull,7275004492439954170ull,16274625055262694174ull,9100812775640660847ull,10611801488751952495ull,13988464420037366691ull,12715906540348551121ull,11766429052248709786ull,3338715427749605ull,2190861738386064756ull,2380874473443914065ull,5805871478722753955ull,18380152411992198484ull,17753836161221930124ull,558899075728331952ull,6945707259485057305ull,15540448118791785514ull,14100396407684167764ull,7686576470926131642ull,6847113332786445596ull,8571497544299597952ull,5107506230492840013ull,13809089677202756103ull,11850464950116242772ull,8665410045503026098ull,12611995970725331088ull,14716086967124651162ull,8332337353906700486ull,1055042741385964424ull,15747416080677248841ull,14915670236539320530ull,7900308686393206650ull,4979886344023150111ull,5902402781825910141ull,3255426766738440460ull,15073047456619535065ull,14385299733787058194ull,9204231696702233102ull,10378053593247535553ull,17743937090908512033ull,284875276098321550ull,8191444072549517566ull,11831960808665605673ull,16522251628765377415ull,9489110493662951639ull,2985165104683868371ull,1923666239526938648ull,2608156467518575538ull,15010176170111705563ull,8595623925997760729ull,5537255266788969170ull,4026706972241118255ull,10146297908886518879ull,11571719523540239514ull,8947543025284496775ull,6474440949527995615ull,8042516419360849959ull,12772931646054458863ull,8362173284515136314ull,2965170807480292156ull,3697936742835324831ull,2548596695355670143ull,678615102317890017ull,18347860548317292650ull,5348024243900531994ull,
                                       }),
                  createColumn<UInt64>({1963993824751904883ull,8092151856919135067ull,5131775057722478077ull,15715528133814023746ull,15505304522311288171ull,10286790629013975231ull,1501823745926746029ull,12973914748851182578ull,8210074373542591108ull,13460194198694121125ull,5224676877811702912ull,9335603723497191999ull,14816589124295502018ull,4954279722715295418ull,11100674508285551899ull,13464602075238859455ull,16853392590959687352ull,1817058812112508107ull,15556867487501561584ull,10284485237014856710ull,14437749781215352280ull,5771839681340207927ull,16055484011087334393ull,3176481359853048004ull,7814936607178752912ull,2494931346489092259ull,8767088596499268464ull,14226279241180503276ull,7473321867985919185ull,6734041306837931713ull,13235889048871924509ull,10991267203008554602ull,16751923766106295286ull,10095830120191421323ull,16222110847614790525ull,9842614984677906240ull,4933809552431881486ull,8405192294344557484ull,4959648925715618956ull,6697802956154799897ull,12131820337280328334ull,7842863996271662529ull,16569735361616664303ull,12858241103410228367ull,5513953872175351416ull,859413842831558633ull,13344669626051684726ull,13723621615205909176ull,15587646588258269977ull,14455229124733503750ull,1897402925930790108ull,15359004855099545708ull,12520174369297951929ull,10652815703651083579ull,8168743478119503616ull,13222025305950777542ull,1407449211367397936ull,719150994271271568ull,13201069077204373904ull,7881062513337573241ull,505835289877393142ull,16274255003040284083ull,5851222720449930723ull,10184171151253590546ull,10760806328248813948ull,11613887821236610307ull,396890304498933892ull,2817599718757852634ull,206272906874991324ull,11706414073185557053ull,1232629356308009454ull,11720363557979026158ull,7397814221360500092ull,17105630191392625269ull,17759165119137150028ull,11745869976822508796ull,13060766979359736635ull,6099807408799721275ull,9051766638104113010ull,9942483311080030083ull,5022375889081250419ull,16686942398742544149ull,17812504368324647606ull,14378217067310861473ull,13056638079919822910ull,6759774801955234976ull,12848167735234317960ull,532081090242244444ull,6210198885610140778ull,12902504877688878394ull,16860750806044045906ull,9057877269281939547ull,6387131386245764054ull,14021225438163328133ull,17155620859144768173ull,5353199724262717805ull,2748605048746777638ull,18127535939063270402ull,7104584635051455285ull,12463829834478575910ull,
                                       }),
                  createColumn<UInt64>({162130378342041698ull,2306970147751363337ull,4906399183902082525ull,5192791187330961984ull,13907124735635686698ull,9962525953971236925ull,346918583778004225ull,11532613229130318130ull,1243004632125550596ull,13298056637815390724ull,36528456736988288ull,1726331243280944ull,9872189512905173186ull,36347689790344848ull,144256074823770384ull,3625644066346730142ull,6953842598776156216ull,1225956686928544906ull,4667154241155113152ull,10241542350102922244ull,4616489114865270864ull,5767017669974884913ull,14127801515980751761ull,281625320505476ull,3198685346327036304ull,146085883425597440ull,1272513185338262384ull,14153799398103144456ull,2465177654445579456ull,1825101351191904385ull,13198379756955640345ull,10988802093375644160ull,14446070136962059092ull,871868740806920456ull,16222109997211058252ull,9804857694914446912ull,4931441594151796746ull,6953862676154024716ull,4919078683284953100ull,1170971123190399497ull,9223374512903684738ull,2330455578176390593ull,11619991280813265546ull,426645963123845ull,866099620573742192ull,74309462839738689ull,1157574982499393826ull,13695456519373488144ull,15006629391026759688ull,36321322981392384ull,19151158298030104ull,15357838821927438376ull,9331608240669354000ull,181309540438966586ull,5838094792777667840ull,3923780495183728768ull,180145741880360960ull,694310827035464192ull,11831253089948602640ull,7512330184322847536ull,505536205530431632ull,13841813592565902482ull,5846279246746534082ull,865889046541369856ull,10376856859172309320ull,9235775630514724866ull,396889200523485184ull,367043386831602714ull,56440184765546588ull,2316408220766846988ull,1227838705627107528ull,9413113126569910274ull,7397814219170809868ull,9224005362282401857ull,17741122064043496448ull,216195325862358156ull,3531114965719253050ull,297959404870379049ull,7280724381522551554ull,9344973233195333763ull,81346546033008723ull,185258913461970960ull,2608156312547431090ull,13837468399527035009ull,3819134019844542488ull,5532681290774216832ull,3621347271012271112ull,306882631328345180ull,1970980911154186ull,3462149708829635842ull,5321356472062500946ull,7895938256515104771ull,1153625330601771462ull,4612917523078266880ull,2883431871756632620ull,162165595591180557ull,2451085405657799718ull,648765478774114304ull,7097813840969080864ull,590089416916222210ull,
                                       }));
    // clang-format on
}
CATCH


} // namespace tests
} // namespace DB
