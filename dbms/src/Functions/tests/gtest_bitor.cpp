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
class TestFunctionBitOr : public DB::tests::FunctionTest
{
};

#define ASSERT_BITOR(t1, t2, result) ASSERT_COLUMN_EQ(result, executeFunction("bitOr", {t1, t2}))

TEST_F(TestFunctionBitOr, Simple)
try
{
    ASSERT_BITOR(
        createColumn<Nullable<Int64>>({-1, 1}),
        createColumn<Nullable<Int64>>({0, 0}),
        createColumn<Nullable<UInt64>>({UINT64_MAX, 1}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitOr, others will be casted by TiDB Planner.
TEST_F(TestFunctionBitOr, TypePromotion)
try
{
    // Type Promotion
    ASSERT_BITOR(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<Int32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<Int32>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITOR(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<UInt16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    // Type Promotion across signed/unsigned
    ASSERT_BITOR(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<Int64>>({1}),
        createColumn<Nullable<UInt8>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitOr, Nullable)
try
{
    // Non Nullable
    ASSERT_BITOR(createColumn<Int8>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<Int16>({1}), createColumn<Int32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<Int32>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<Int8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITOR(createColumn<UInt8>({1}), createColumn<UInt16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<UInt16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<UInt32>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<UInt8>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITOR(createColumn<Int16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<Int64>({1}), createColumn<UInt8>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<UInt32>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITOR(createColumn<UInt8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    // Across Nullable and non-Nullable
    ASSERT_BITOR(createColumn<Int8>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Int16>({1}), createColumn<Nullable<Int32>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Int32>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Int8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITOR(createColumn<UInt8>({1}), createColumn<Nullable<UInt16>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<UInt16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<UInt32>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<UInt8>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITOR(createColumn<Int16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Int64>({1}), createColumn<Nullable<UInt8>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<UInt32>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<UInt8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITOR(createColumn<Nullable<Int8>>({1}), createColumn<Int16>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<Int16>>({1}), createColumn<Int32>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<Int32>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<Int8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITOR(createColumn<Nullable<UInt8>>({1}), createColumn<UInt16>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<UInt16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<UInt32>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<UInt8>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITOR(createColumn<Nullable<Int16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<Int64>>({1}), createColumn<UInt8>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<UInt32>>({1}), createColumn<Int16>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITOR(createColumn<Nullable<UInt8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitOr, TypeCastWithConst)
try
{
    /// need test these kinds of columns:
    /// 1. ColumnVector
    /// 2. ColumnVector<Nullable>
    /// 3. ColumnConst
    /// 4. ColumnConst<Nullable>, value != null
    /// 5. ColumnConst<Nullable>, value = null

    ASSERT_BITOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 1, 1, 1}));
    ASSERT_BITOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITOR(createColumn<Int8>({0, 0, 1, 1}), createConstColumn<UInt64>(4, 0), createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt)); // become const in wrapInNullable

    ASSERT_BITOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITOR(createConstColumn<Int8>(4, 0), createColumn<UInt64>({0, 1, 0, 1}), createColumn<UInt64>({0, 1, 0, 1}));
    ASSERT_BITOR(
        createConstColumn<Int8>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITOR(createConstColumn<Int8>(4, 0), createConstColumn<UInt64>(4, 0), createConstColumn<UInt64>(4, 0));
    ASSERT_BITOR(
        createConstColumn<Int8>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITOR(
        createConstColumn<Int8>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 1, 0, 1}));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<UInt64>({0, 1, 0, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitOr, Boundary)
try
{
    ASSERT_BITOR(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt8>({0, 255, 0, 255}),
        createColumn<UInt64>({127, 255, 18446744073709551488ull, UINT64_MAX}));
    ASSERT_BITOR(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt16>({0, 65535, 0, 65535}),
        createColumn<UInt64>({127, 65535, 18446744073709551488ull, UINT64_MAX}));
    ASSERT_BITOR(
        createColumn<Int16>({32767, 32767, -32768, -32768}),
        createColumn<UInt8>({0, 255, 0, 255}),
        createColumn<UInt64>({32767, 32767, 18446744073709518848ull, 18446744073709519103ull}));

    ASSERT_BITOR(
        createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>(
            {0,
             UINT64_MAX,
             1,
             UINT64_MAX,
             UINT64_MAX,
             UINT64_MAX,
             INT64_MAX,
             UINT64_MAX,
             9223372036854775808ull,
             UINT64_MAX}));
}
CATCH

TEST_F(TestFunctionBitOr, UINT64)
try
{
    ASSERT_BITOR(
        createColumn<UInt64>({0, 0, UINT64_MAX, UINT64_MAX}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, UINT64_MAX, UINT64_MAX, UINT64_MAX}));

    ASSERT_BITOR(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, 0, UINT64_MAX, std::nullopt, 0}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, UINT64_MAX, UINT64_MAX, std::nullopt, std::nullopt}));

    ASSERT_BITOR(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, UINT64_MAX, UINT64_MAX, std::nullopt}));

    ASSERT_BITOR(
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, UINT64_MAX, UINT64_MAX, std::nullopt}));

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
        res[i] = v1[i] | v2[i];
    }
    */
    // clang-format off
    ASSERT_BITOR(createColumn<UInt64>({10449567999524462821ull,5115522722838073307ull,13099136386244709031ull,8172138952570148226ull,2952491142199712182ull,17553707191021549516ull,3722632758014817277ull,13648626705296851269ull,7091568482883034785ull,6647616532132710745ull,1338631936359925232ull,8548188144813167418ull,9287855045671651824ull,7086954459021487113ull,2950028798684172464ull,10608209797490141524ull,4603087763124269210ull,327552405535575359ull,9560137416190405998ull,17992212654875872830ull,9882274054852774680ull,15468900524578002670ull,7783830593941196727ull,14257273361186091349ull,15812789496639889683ull,10085436352750399886ull,6868589075412047520ull,5278469436602073267ull,10235247517869902216ull,9391265748974951308ull,2889814595277585729ull,1332829663676817400ull,12269370365179677499ull,2132105913418909949ull,4529533638037066630ull,12599846282599304011ull,11219102585011603246ull,10620727498988293561ull,14315969389478775512ull,5299385651679653771ull,12739906169428040247ull,13717684203244918752ull,7624489218003262332ull,12137396359308560662ull,12859097101250082790ull,15442280237235690290ull,6481405670886635344ull,11117175020607576758ull,1098382684876606257ull,10607107250581664997ull,7715653090546292451ull,1998576479269610293ull,12045070629924495310ull,7949595659894489715ull,10446400532077651448ull,947933239526727685ull,20023133392409237ull,7326526749795551806ull,10538633649786511376ull,7046047352274845581ull,4919357210382932698ull,1791343629612779866ull,8832281373132352128ull,13150574980027373537ull,13339030213107123610ull,5050947496516539288ull,17210305094325277394ull,1408211759885506339ull,2885482844143333010ull,17036462956721694661ull,16963950168065011583ull,11522676888160847598ull,5363596045579091425ull,15030621977731711350ull,7967689812631184957ull,14007814294777575597ull,11000092392700571573ull,3690350887970038751ull,2714068264122136867ull,9244234272373894330ull,5742134076008119701ull,8667639910282803550ull,15851917698209440286ull,12534411906883668930ull,3177478831968313424ull,15251203006522183128ull,14753115920897975016ull,6916265614214068154ull,7891474206479313221ull,13765957528127150768ull,7143013174003796592ull,125585053146203112ull,3036036215727088180ull,7397683420519048696ull,223091748914600207ull,15597982284062956733ull,10390590993119554509ull,2379894200470404684ull,864494681651604513ull,13548704093568709848ull,
                                       }),
                  createColumn<UInt64>({1055421217716118351ull,8135701836280126115ull,9842076143095254006ull,8470455635198845160ull,3130232706619718443ull,11517740503859024ull,3822013156361433125ull,11994892560198506246ull,15864965286225865340ull,6786652356367467229ull,16614133826709464896ull,5547075729638343399ull,11722212948149790525ull,12727020249062448895ull,10877976181965365451ull,2653779792189331119ull,9097738068698707564ull,3111766523835781174ull,11678908815419699034ull,16415148725799048677ull,10798546194341708507ull,5363963935689912203ull,9628442107398126594ull,12657489553508989072ull,6220836380674986050ull,14723664668690905385ull,7644730870124555323ull,13129591855668977564ull,5163613600967689692ull,7348190532725961041ull,10303122894857538354ull,6090156108301688457ull,15031600157166973366ull,15709866272892400093ull,8703598464199746194ull,5667250243507043984ull,10224176409178812535ull,495677073288872842ull,13409064908298367053ull,15122756481620324611ull,2649751259364459464ull,11542110491971064760ull,3773278509687378481ull,10968611370472621015ull,3020086332867667993ull,5955503430749948367ull,3251132896626509659ull,9153029141732703312ull,8156945596271758286ull,9817263315493727495ull,2486944934742981862ull,14182382307783808429ull,8710796998481323432ull,12232862702277924175ull,294496145998592599ull,7216960705867649127ull,11269828075967756564ull,12336122210566559010ull,16209893786882404812ull,7852700419184773615ull,14973812202590143035ull,27218272642815317ull,7163734061082901889ull,12902821926617583885ull,5232271947234447267ull,16460259165874244638ull,4289652883449807862ull,14337199046890646289ull,9955648001928482530ull,5272403679590674251ull,7398977048567546811ull,5916703570842976230ull,10585500772257466084ull,247967539942328313ull,15557191652119929412ull,15435588854332648849ull,14971407198818754212ull,12394514370525038982ull,13197898454997160459ull,1760194372821020067ull,18152169899605582485ull,4028291602721493003ull,9326699944561020940ull,4111281404912103329ull,13165869039324514156ull,13528768436403206709ull,15615380725821152138ull,13185439239486997020ull,15313731711838731253ull,1573545848664404247ull,14294766714110870879ull,5047188605646201129ull,12083575114134784992ull,17160901467798567200ull,5910074161931627108ull,17005786097992719580ull,7371174878626772709ull,2378189191861071822ull,11021336104146126819ull,10307446632517413474ull,
                                       }),
                  createColumn<UInt64>({11503845720007500783ull,8574851453908934651ull,13681794103137438711ull,8497553169226380778ull,3168804674051357631ull,17562890656326615004ull,4011571434894586877ull,13799006992510719815ull,18333010588314992381ull,6804688760418716637ull,17767763447945865200ull,9150025814514586623ull,11740249767194128381ull,17500988136926920447ull,13760431727199564027ull,13256344108034686975ull,9214923385195050750ull,3436166457181855039ull,12015603757237137278ull,18158371721801850879ull,11384228962993392603ull,16067915310574862319ull,17122608540422176695ull,17292819539339180501ull,16102297206815437139ull,14985446206908397999ull,9175991329572585147ull,18407882930164522943ull,14964044805785488860ull,16716937935470328797ull,12609709242688995187ull,6268937799966089209ull,18077448869901106111ull,16111323291863235069ull,9141576037851984790ull,17221694056497397723ull,11525787173584942975ull,10945154399298375611ull,18356280103110949597ull,15699535027144490891ull,13028350478857067519ull,13726920395125069816ull,9070215410999712637ull,13292644711307463639ull,13546177519427190783ull,15487372102444506111ull,9078936061688084315ull,18396702859919867638ull,9169188101689936895ull,11186360902175423975ull,7751794195109510887ull,16140337810933413821ull,18440833099184472046ull,17281433216112105343ull,10736392325857525759ull,7865536501704536167ull,11270111762855050133ull,17275735375175114558ull,17507110198310859228ull,7924919161384206319ull,14973830897108491003ull,1800511725594664287ull,8933649887195463553ull,13227136354618169325ull,17986747850397487035ull,16608879588366180254ull,18435460739219821558ull,15564439979198054195ull,12262135615271202546ull,17109174983818473423ull,17288895574215716863ull,16140021029948850158ull,15775919277949845477ull,15273957234283366399ull,18444210712204535421ull,15454096531153223101ull,16135623161914195893ull,13778459279832837087ull,13234494620821977899ull,10984138169552170427ull,18444913240781062037ull,9218866100495186271ull,15852489474324263454ull,13690729285990379491ull,13744630008684177276ull,18140498710191144957ull,15906145212138634218ull,18446629651477362622ull,18268128392595078133ull,13825729797750176695ull,16672667454839623551ull,5169921248573055977ull,12660599160008043508ull,17199209552694607352ull,5988905573643860847ull,18192214278426713341ull,17761052888497913837ull,2379896744190017486ull,11240966936799997923ull,13767480558416248570ull,
                                       }));
    // clang-format on
}
CATCH

} // namespace tests
} // namespace DB
