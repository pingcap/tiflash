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
class TestFunctionBitXor : public DB::tests::FunctionTest
{
};

#define ASSERT_BITXOR(t1, t2, result) ASSERT_COLUMN_EQ(result, executeFunction("bitXor", {t1, t2}))

TEST_F(TestFunctionBitXor, Simple)
try
{
    ASSERT_BITXOR(
        createColumn<Nullable<Int64>>({-1, 1}),
        createColumn<Nullable<Int64>>({0, 0}),
        createColumn<Nullable<UInt64>>({UINT64_MAX, 1}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitXor, others will be casted by TiDB Planner.
TEST_F(TestFunctionBitXor, TypePromotion)
try
{
    // Type Promotion
    ASSERT_BITXOR(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<Int32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int32>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITXOR(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<UInt16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    // Type Promotion across signed/unsigned
    ASSERT_BITXOR(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int64>>({1}),
        createColumn<Nullable<UInt8>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitXor, Nullable)
try
{
    // Non Nullable
    ASSERT_BITXOR(createColumn<Int8>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<Int16>({1}), createColumn<Int32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<Int32>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<Int8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITXOR(createColumn<UInt8>({1}), createColumn<UInt16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<UInt16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<UInt32>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<UInt8>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITXOR(createColumn<Int16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<Int64>({1}), createColumn<UInt8>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<UInt32>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITXOR(createColumn<UInt8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    // Across Nullable and non-Nullable
    ASSERT_BITXOR(createColumn<Int8>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Int16>({1}), createColumn<Nullable<Int32>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Int32>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Int8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITXOR(createColumn<UInt8>({1}), createColumn<Nullable<UInt16>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<UInt16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<UInt32>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<UInt8>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITXOR(createColumn<Int16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Int64>({1}), createColumn<Nullable<UInt8>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<UInt32>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<UInt8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITXOR(createColumn<Nullable<Int8>>({1}), createColumn<Int16>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<Int16>>({1}), createColumn<Int32>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<Int32>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<Int8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITXOR(createColumn<Nullable<UInt8>>({1}), createColumn<UInt16>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<UInt16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<UInt32>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<UInt8>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITXOR(createColumn<Nullable<Int16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<Int64>>({1}), createColumn<UInt8>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<UInt32>>({1}), createColumn<Int16>({0}), createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITXOR(createColumn<Nullable<UInt8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitXor, TypeCastWithConst)
try
{
    /// need test these kinds of columns:
    /// 1. ColumnVector
    /// 2. ColumnVector<Nullable>
    /// 3. ColumnConst
    /// 4. ColumnConst<Nullable>, value != null
    /// 5. ColumnConst<Nullable>, value = null

    ASSERT_BITXOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 1, 1, 0}));
    ASSERT_BITXOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITXOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<UInt64>(4, 0),
        createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITXOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITXOR(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt)); // become const in wrapInNullable

    ASSERT_BITXOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITXOR(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITXOR(
        createConstColumn<Int8>(4, 0),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 1, 0, 1}));
    ASSERT_BITXOR(
        createConstColumn<Int8>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITXOR(createConstColumn<Int8>(4, 0), createConstColumn<UInt64>(4, 0), createConstColumn<UInt64>(4, 0));
    ASSERT_BITXOR(
        createConstColumn<Int8>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITXOR(
        createConstColumn<Int8>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 1, 0, 1}));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 0));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<UInt64>({0, 1, 0, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITXOR(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitXor, Boundary)
try
{
    ASSERT_BITXOR(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt8>({0, 255, 0, 255}),
        createColumn<UInt64>({127, 128, 18446744073709551488ull, 18446744073709551487ull}));
    ASSERT_BITXOR(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt16>({0, 65535, 0, 65535}),
        createColumn<UInt64>({127, 65408, 18446744073709551488ull, 18446744073709486207ull}));
    ASSERT_BITXOR(
        createColumn<Int16>({32767, 32767, -32768, -32768}),
        createColumn<UInt8>({0, 255, 0, 255}),
        createColumn<UInt64>({32767, 32512, 18446744073709518848ull, 18446744073709519103ull}));

    ASSERT_BITXOR(
        createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>(
            {0,
             UINT64_MAX,
             1,
             UINT64_MAX - 1,
             UINT64_MAX,
             0,
             INT64_MAX,
             9223372036854775808ull,
             9223372036854775808ull,
             INT64_MAX}));
}
CATCH

TEST_F(TestFunctionBitXor, UINT64)
try
{
    ASSERT_BITXOR(
        createColumn<UInt64>({0, 0, UINT64_MAX, UINT64_MAX}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, UINT64_MAX, UINT64_MAX, 0}));

    ASSERT_BITXOR(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, 0, UINT64_MAX, std::nullopt, 0}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, UINT64_MAX, 0, std::nullopt, std::nullopt}));

    ASSERT_BITXOR(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}));

    ASSERT_BITXOR(
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}));

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
        res[i] = v1[i] ^ v2[i];
    }
    */
    // clang-format off
    ASSERT_BITXOR(createColumn<UInt64>({14718299460514655645ull,13653458517568093814ull,873343573168268054ull,4513397769233815805ull,10672257483585440709ull,1175657678393289426ull,5310100026964525133ull,2961032951668734270ull,3488618122219137050ull,17743416490532629047ull,13718689122891849443ull,18201153781905814305ull,1128496818330993343ull,13744157299602302676ull,9503005189764910889ull,1902802679836938440ull,11568697473643700541ull,1965116275151063349ull,15426737199788150516ull,3920422261009228254ull,219130970446445094ull,1067134948824549795ull,7229380357343545562ull,10249979280443944135ull,5247619042224604597ull,10935277574815179355ull,7452298659045164021ull,13014695302102943439ull,17774455674396635602ull,11703102532100082512ull,4546922703539587451ull,14346913699564568234ull,14622996970487333807ull,4988534825775753314ull,15407125419828298011ull,9943452973350258867ull,2226676981676525386ull,9937768782093284791ull,1495999957992218739ull,1481151937683438688ull,12938037958808022024ull,4650438177133561636ull,1916445663045189124ull,14242046937360146371ull,15292683658227630884ull,15742489248107441478ull,11556191801920357467ull,5237942344222510697ull,13184031405340938242ull,18380112867858045508ull,17693484540370322729ull,15196301976284697948ull,2507523577892575787ull,5959236094880586462ull,15637713062286618837ull,2739731215516370624ull,6361345912389037249ull,15245253943880573013ull,4091873252779897041ull,2636879545390540064ull,9947865817470934117ull,10222529134713022152ull,12976542357761309939ull,17961043577121718626ull,17241516921982517932ull,12088414360545693338ull,12483717921162173311ull,11164999382092476714ull,4214074118384652354ull,12176001461824278835ull,10730602597265564381ull,6596740911321671783ull,7943160227997394463ull,4150298599006492052ull,55824340661557068ull,10452689460625698514ull,9698018763533898729ull,8435261915760742117ull,17917927325247541708ull,963296348376816136ull,947283174038418576ull,2989280348280108383ull,639699486765298192ull,674726098288966021ull,16239183043384654438ull,187922631166157998ull,7388395401411446843ull,11335527333270277802ull,1308106126215629344ull,3812712442281394199ull,16551852854811672277ull,7034049596692493428ull,7972559625647453148ull,11212208881240934516ull,11003286996850584466ull,17708706021111278984ull,12752755184457789942ull,5629304106454051668ull,11864803465634504202ull,4350267639367961198ull,
                                        }),
                 createColumn<UInt64>({13689784535080939867ull,10635492095641807209ull,13669002141479868424ull,13243335774353039557ull,15593052356118731762ull,11458751298332845275ull,16237908667491138210ull,3783130141132616623ull,5450382678095848786ull,12136362746458950399ull,12844741012345994361ull,10735219206054823336ull,7640413713722963501ull,716277869975200817ull,16906496758641729445ull,4228216149605694539ull,15326286314654316754ull,17945299799453987017ull,4228089023246991468ull,13009774985688653741ull,779314342091711911ull,2742764211561362224ull,13674473000646067343ull,12948053999619850276ull,10397472234170949254ull,16949517779117525634ull,14691170915436020022ull,12885651788555070543ull,5376518716527066364ull,11126270203629316903ull,3421367740741913281ull,13562756397172559670ull,15620120362533800466ull,1640266805595802619ull,78098043989597492ull,17408499809945336449ull,9347129437132344979ull,6454847367145914064ull,1390655705199753457ull,12128425210371145484ull,15908310214987987634ull,16416014720889051318ull,456660366263885142ull,4066620820656610476ull,3753918687130272881ull,16926641938948443294ull,11802634551801696760ull,14527708511994589302ull,13562757450783392080ull,14066479082287644331ull,13032779374389778466ull,6049654127840253218ull,10505008616820918931ull,14810325095060711898ull,7978597267399072938ull,2292396566499160988ull,1229743880651968023ull,9181402355992000223ull,10478929079695685694ull,6838997434878913829ull,12205179176437992596ull,13108441474265613992ull,2268879948202530790ull,12533883824564291161ull,3759576559606688815ull,10575286785954536779ull,3913938763954127275ull,14148543828733505057ull,16233215773996940422ull,1244518395821524943ull,2783683841239726556ull,15658155554083911219ull,2170079932517134431ull,14269662392227229929ull,15911322703155660912ull,14423990520180144757ull,2023494476338002369ull,12730745377249631743ull,5590943603965359458ull,11818647802799353851ull,5809409268922892307ull,1379629206665120101ull,4315261715191415916ull,10746915376657554893ull,12890172056943936171ull,134407234537979606ull,11169634876582902567ull,17097022742711365386ull,14647374886056646441ull,9830306787447872369ull,14942915785115825174ull,5079042523991294302ull,5353560875203612104ull,8849045523168991468ull,4280435333895018913ull,7029078760928811301ull,14861899004351266765ull,8976835935073334848ull,4755268395840913565ull,12551449787642431832ull,
                                      }),
                 createColumn<UInt64>({8194868932271545542ull,3378279818324416287ull,12802801753940998942ull,9902017980423862328ull,5512088338554359863ull,10328284421601054217ull,12171505971151567599ull,2132405056353897105ull,8919847014834163016ull,6796005332175364296ull,874609788107743898ull,7524481521401050761ull,7323145645353464978ull,13208412100675453669ull,7601660438660747404ull,2361496831160396931ull,8376041357077862895ull,16307531785076488700ull,17058499794863967896ull,9431701099934465651ull,709928506066296705ull,2945204699035564691ull,15678885864300858453ull,4435912985144421603ull,15607352463913425715ull,9005265876912482521ull,12432859918252147395ull,454428619380959872ull,13562227482584927534ull,4035707349117853815ull,1180727943039927226ull,8872916907634438556ull,1309002614312962493ull,6051061116251956121ull,15331501289924647983ull,8676562485192897074ull,11479977102690781657ull,15023504184298834791ull,544449535631235202ull,13609536878441186156ull,8018816518568941242ull,11770380327700848530ull,2075868067351356242ull,18287534980944577391ull,16150506309778507605ull,3503534126870946264ull,258031880943359395ull,9307850971667709471ull,778998202728370514ull,4333988378890757359ull,4708049108700093707ull,9300122630992478334ull,12899939656307790008ull,11473955323386966788ull,13240014305160408191ull,4167309109657793884ull,5285229163713422038ull,12463959892971591306ull,12224426839583197423ull,8822811255690283013ull,2552532233173848305ull,4050761399586988128ull,12351507697159531285ull,6103289087308695355ull,15810623975326379651ull,3819153215501194193ull,11199900178395077332ull,6821742250119618315ull,15797661745128336580ull,13383768122063705852ull,12847471772000541441ull,9421712305619885652ull,8081263232408917568ull,18415452804545062269ull,15858876621726441276ull,6423020768960976039ull,11133651649318726184ull,14248443079914017562ull,13059953407232435374ull,12203089658902492659ull,6753789706493850755ull,4204501505355414586ull,3675580379533080188ull,11275233734257386568ull,6034328448597653709ull,235911256832907896ull,18269513542380811036ull,8073601272631763360ull,15664291695543590153ull,13584327848764906342ull,3092874280798669507ull,2873741396702868778ull,2661508621189692948ull,16237698316423860376ull,11805252354203447859ull,10686403825869206701ull,9131876326791581243ull,3642126747183783188ull,16525436061620018839ull,10552202526985536310ull,
                                      }));
    // clang-format on
}
CATCH

} // namespace tests
} // namespace DB
