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

#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestFunctionBitShiftRight : public DB::tests::FunctionTest
{
};

#define ASSERT_BITSHIFTRIGHT(t1, t2, result) ASSERT_COLUMN_EQ(result, executeFunction("bitShiftRight", {t1, t2}))

TEST_F(TestFunctionBitShiftRight, Simple)
try
{
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int64>>({8}),
        createColumn<Nullable<Int64>>({2}),
        createColumn<Nullable<UInt64>>({2}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitShiftRight, others will be casted by TiDB planner.
/// Note: BitShiftRight will further cast other types to UInt64 before doing shift.
TEST_F(TestFunctionBitShiftRight, TypePromotion)
try
{
    // Type Promotion
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({-1}),
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<UInt64>>({9223372036854775807ull}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int16>>({-1}),
        createColumn<Nullable<Int32>>({1}),
        createColumn<Nullable<UInt64>>({9223372036854775807ull}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int32>>({-1}),
        createColumn<Nullable<Int64>>({1}),
        createColumn<Nullable<UInt64>>({9223372036854775807ull}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({-1}),
        createColumn<Nullable<Int64>>({1}),
        createColumn<Nullable<UInt64>>({9223372036854775807ull}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    // Type Promotion across signed/unsigned
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int16>>({-1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({18446744073709551615ull}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int64>>({-1}),
        createColumn<Nullable<UInt8>>({0}),
        createColumn<Nullable<UInt64>>({18446744073709551615ull}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitShiftRight, Nullable)
try
{
    // Non Nullable
    ASSERT_BITSHIFTRIGHT(createColumn<Int8>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<Int16>({1}), createColumn<Int32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<Int32>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<Int8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITSHIFTRIGHT(createColumn<UInt8>({1}), createColumn<UInt16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt32>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt8>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITSHIFTRIGHT(createColumn<Int16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<Int64>({1}), createColumn<UInt8>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt32>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    // Across Nullable and non-Nullable
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int16>({1}),
        createColumn<Nullable<Int32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int32>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt8>({1}),
        createColumn<Nullable<UInt16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt16>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt32>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt8>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Int16>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int64>({1}),
        createColumn<Nullable<UInt8>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt32>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt8>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Int16>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Int32>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int32>>({1}),
        createColumn<Int64>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Int64>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<UInt16>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt16>>({1}),
        createColumn<UInt32>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<UInt64>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<UInt64>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int16>>({1}),
        createColumn<UInt32>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int64>>({1}),
        createColumn<UInt8>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Int16>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Int64>({0}),
        createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitShiftRight, TypeCastWithConst)
try
{
    /// need test these kinds of columns:
    /// 1. ColumnVector
    /// 2. ColumnVector<Nullable>
    /// 3. ColumnConst
    /// 4. ColumnConst<Nullable>, value != null
    /// 5. ColumnConst<Nullable>, value = null

    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 0, 1, 0}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<UInt64>(4, 0),
        createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt)); // become const in wrapInNullable

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Int8>(4, 1),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({1, 0, 1, 0}));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Int8>(4, 1),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({1, 0, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Int8>(4, 1),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Int8>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Int8>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({1, 0, 1, 0}));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({1, 0, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<UInt64>({0, 1, 0, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTRIGHT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitShiftRight, Boundary)
try
{
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt8>({0, 7, 0, 7}),
        createColumn<UInt64>({127, 0, 18446744073709551488ull, 144115188075855871ull}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt16>({0, 7, 0, 7}),
        createColumn<UInt64>({127, 0, 18446744073709551488ull, 144115188075855871ull}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int16>({32767, 32767, -32768, -32768}),
        createColumn<UInt8>({0, 15, 0, 15}),
        createColumn<UInt64>({32767, 0, 18446744073709518848ull, 562949953421311ull}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
        createColumn<UInt64>({0, 63, 0, 63, 0, 63, 0, 63, 0, 63}),
        createColumn<UInt64>({0, 0, 1, 0, 18446744073709551615ull, 1, INT64_MAX, 0, 9223372036854775808ull, 1}));
}
CATCH

TEST_F(TestFunctionBitShiftRight, UINT64)
try
{
    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt64>({0, UINT64_MAX}),
        createColumn<UInt64>({63, 63}),
        createColumn<UInt64>({0, 1}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, std::nullopt}),
        createColumn<Nullable<UInt64>>({63, 63, 63}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, std::nullopt}),
        createColumn<UInt64>({63, 63, 63}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt64>({0, UINT64_MAX}),
        createColumn<Nullable<UInt64>>({63, 63}),
        createColumn<Nullable<UInt64>>({0, 1}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, 0, 1, 0, 18446744073709551615ull, 0, INT64_MAX, 0, 9223372036854775808ull, 0}));


    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt64>({0, 0, UINT64_MAX, UINT64_MAX}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, 0, UINT64_MAX, 0}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, 0, UINT64_MAX, std::nullopt, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, 0, std::nullopt, std::nullopt}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, 0, std::nullopt}));

    ASSERT_BITSHIFTRIGHT(
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, 0, 0, std::nullopt}));

    /*
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dis(
            std::numeric_limits<std::uint64_t>::min(),
            std::numeric_limits<std::uint64_t>::max()
    );
    size_t count = 100;
    std::vector<uint64_t> v1(count), v2(count), res(count);
    for (size_t i=0; i<count; ++i) {
        v1[i] = dis(gen);
        v2[i] = dis(gen) % 64;
        res[i] = v1[i] >> v2[i];
    }
    */
    // clang-format off
    ASSERT_BITSHIFTRIGHT(createColumn<UInt64>({4286230172992429668ull,11550684080080434735ull,775195682263841867ull,18390588538388462661ull,15578761645824658314ull,20662948907547635ull,8403266546632871011ull,10316916867086714284ull,14494183568060929367ull,11741337603037632348ull,10803264694948981380ull,2181969932373516503ull,9673801579564730047ull,12998855911221966916ull,13852157931865274857ull,9203926828777338586ull,8903261359104369984ull,3296258311466476456ull,14658801806079697908ull,7542518003247963618ull,7751150277360944372ull,12225694156629117269ull,3173837214287201256ull,10555082060194839563ull,14202570947308501213ull,13841194359225980123ull,9085267378073816945ull,15975493157631073381ull,1890233386459299033ull,2368634323417847398ull,691423931511513606ull,986000479038857169ull,6676906740954304741ull,2841686799872009560ull,6483676442160212821ull,12550114481083571140ull,1973026146580965947ull,15006687639313690830ull,6443617813685195609ull,13648732879238232658ull,173820604016606515ull,2669428687588070677ull,15361476519767969236ull,8957522718906827285ull,10484385204137290737ull,12390466571993898199ull,13655746682011856065ull,4183302523705398003ull,9898692767945122925ull,16701902679050716746ull,15003324714492513897ull,15554724240808081962ull,7754458312088240871ull,16060968032680196798ull,12619581440986221928ull,15462661961676206824ull,2991773628650321635ull,16341599119345297909ull,14943939970889580769ull,17589764776976679210ull,15274914527536421890ull,16268454608136611433ull,14617646699124891378ull,466927094873143934ull,10558583305251737283ull,255559140356160501ull,5962789691899784330ull,8004603198837555992ull,1881892337023478820ull,6549167700870881840ull,17551996157828573642ull,3349744237253314638ull,2876698686583880568ull,16792783373922568330ull,16231348759981899800ull,17731631990557975899ull,1305376485657663531ull,3568754485566225727ull,10076204423028931225ull,1206238310176455071ull,4297062324543635867ull,5116785256928623516ull,4216305034157620433ull,412817651268481791ull,11256299741838589766ull,10786197076871163667ull,8588357635228913652ull,6361409982074778071ull,4750871994764527580ull,12851835128796581697ull,13871712051825681122ull,12445309465661589227ull,1668617678034382020ull,10152918068481134781ull,16242941973571224246ull,12988338226657152812ull,2352083670492692674ull,10735026236980245779ull,14986388012066843516ull,17651064432466444102ull}),
                         createColumn<UInt64>({0,58,55,24,5,35,34,54,43,45,17,36,51,54,19,55,55,8,37,49,15,11,36,0,5,41,46,54,2,59,11,25,43,29,31,8,59,2,11,19,56,35,57,13,2,35,6,54,17,0,49,5,15,3,60,44,16,6,57,44,58,54,26,23,58,23,26,29,56,40,45,2,21,9,57,40,4,46,17,15,62,21,5,54,22,47,10,24,53,61,43,52,23,10,61,43,26,31,38,2}),
                         createColumn<UInt64>({4286230172992429668ull,40,21,1096164497041ull,486836301432020572ull,601370,489134489,572,1647797,333708,82422368583289ull,31751841,4296,721,26420894492846ull,255,247,12876009029165923ull,106656820,13398,236546334147978ull,5969577224916561ull,46185410,10555082060194839563ull,443830342103390662ull,6294246,129109,886,472558346614824758ull,4,337609341558356ull,29385104150ull,759076,5293054133ull,3019197118ull,49023884691732699ull,3,3751671909828422707ull,3146297760588474ull,26032891996838ull,2,77690599,106,1093447597522806ull,2621096301034322684ull,360610038,213371041906435251ull,232,75521032470284ull,16701902679050716746ull,26651,486085132525252561ull,236647287356208ull,2007621004085024599ull,10,878950,45650842722325ull,255337486239770279ull,103,999862,52,903,217819909738ull,55662047251ull,36,30465023560ull,88852490364ull,14909735319ull,26,5956433,498857,837436059313328659ull,1371716826717ull,32798405027192516ull,112,16126825,81586030353603970ull,50715,76875338920813ull,36811471868177ull,0,2439873341049ull,131759532317425638ull,22,2683710990390ull,76640,8387068003153235ull,379169582252ull,527,5,1577031,2763,198914727930ull,9914959051251108ull,7,1476603,35048777915ull,4998886136ull,54520161,4412766108116611025ull}));
    // clang-format on
}
CATCH

TEST_F(TestFunctionBitShiftRight, UB)
try
{
    ASSERT_BITSHIFTRIGHT(createColumn<Int8>({127, -128}), createColumn<UInt8>({64, 64}), createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTRIGHT(createColumn<Int8>({127, -128}), createColumn<UInt16>({64, 64}), createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int16>({32767, -32768}),
        createColumn<UInt8>({64, 64}),
        createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int32>({INT32_MAX, INT32_MIN}),
        createColumn<UInt8>({64, 64}),
        createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTRIGHT(
        createColumn<Int64>({INT64_MAX, INT64_MIN}),
        createColumn<UInt8>({64, 64}),
        createColumn<UInt64>({0, 0}));

    ASSERT_BITSHIFTRIGHT(createColumn<UInt8>({255}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt8>({255}), createColumn<UInt16>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt16>({65535}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt32>({UINT32_MAX}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTRIGHT(createColumn<UInt64>({UINT64_MAX}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));

    /*
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dis1(
            std::numeric_limits<std::uint64_t>::min(),
            std::numeric_limits<std::uint64_t>::max()
    );
    std::uniform_int_distribution<uint64_t> dis2(
            64,
            std::numeric_limits<std::uint64_t>::max()
    );
    size_t count = 100;
    std::vector<uint64_t> v1(count), v2(count), res(count);
    for (size_t i=0; i<count; ++i) {
        v1[i] = dis1(gen);
        v2[i] = dis2(gen);
        res[i] = 0;
    }
    */
    // clang-format off
    ASSERT_BITSHIFTRIGHT(createColumn<UInt64>({17563387625296433369ull,5842891814427459261ull,15074502074821508463ull,386435802999553003ull,5487893274931198395ull,8125923807366590570ull,13340330062727071249ull,14908193031091561411ull,296805448857369387ull,8684453485792353774ull,13117933444495098288ull,3225762988982100714ull,11290506757949810556ull,14617912756126856962ull,9479575714707174581ull,11720728318194739598ull,14410575429605211363ull,12068356718035872518ull,80682389916710599ull,11003236134534292734ull,4412447398096224810ull,5331184707993902906ull,13827083432789678788ull,958142831027309576ull,16716461997317184701ull,17128750834581527743ull,11590434571174666313ull,10204342520615148287ull,11067791415848657283ull,17583875436196878829ull,186304014359496415ull,9381729025189804702ull,11502205568225715300ull,16472133582690439104ull,3743303387826342067ull,12860029445868505658ull,2244056593742923769ull,3275687468466891223ull,1545828456957460699ull,14187252460708728077ull,7551907967738536187ull,9754400233340010491ull,16293183350230169116ull,6298812696728711031ull,5915538565572009956ull,2284684518775825662ull,1130711226902262476ull,17158957721471765323ull,4220824385439711070ull,16559772875254313109ull,15397179690017513678ull,6300413832999049491ull,13787530251307637715ull,10132349060092695582ull,10446586881482901699ull,15759779838283537085ull,14402587207027333363ull,5546051719872960161ull,6545031029710296628ull,17407295406267098658ull,4259019625544816073ull,791895457880289787ull,8549227257401578066ull,15246278171168501125ull,1674668228908076954ull,849762797502000057ull,13302651500925764574ull,12438174880334092333ull,17701249772557033303ull,10742459186038873636ull,15671491258945407856ull,9352557101631889001ull,8914093883925002585ull,17935292744735591949ull,606989231583658922ull,6528503454270721815ull,14980539549624989095ull,13765196438235456668ull,3058323869228644592ull,14346577759191739044ull,1543206286382906519ull,1025562312317433790ull,17052896445025268012ull,18349597294988935754ull,17174604730104962524ull,11924965352621110201ull,502032511104181724ull,13845633389643139332ull,15436039204445155412ull,17809579006694175565ull,15166364145138562881ull,14062748599121933798ull,1777457178576774356ull,4985224560472716170ull,3881603168175384251ull,11555031280550342082ull,1252677486917153396ull,8744807353133366467ull,2048964426549800495ull,11945831330508218140ull}),
                         createColumn<UInt64>({7570379165150948640ull,2086259313016069849ull,3606689596671293211ull,14039117280692395662ull,13678665403528829741ull,16069000531561010558ull,18229345530821449414ull,433464578739092378ull,6298872104011095934ull,4518228872693063137ull,14988726875963869472ull,9568218424260764817ull,5383191468426384555ull,8698762658876708752ull,9487599666567205013ull,14370091126330876161ull,10702068376663045773ull,8045701071228357739ull,10878469353312437370ull,3183167829827610494ull,5928881618833110378ull,10410530709181481816ull,249988564503361262ull,13482614555530280987ull,5522946068620734806ull,12797173590813112894ull,14133419908717831141ull,10825732602137508628ull,13271177233899692778ull,1157753039017783757ull,3370600557036147696ull,2957689395775524062ull,11963898745206689513ull,4828931188614542720ull,15157289330857160797ull,369467010700905309ull,6278071805692607460ull,17817858137511910604ull,17789013631125929528ull,2861684947245777353ull,2583152408663154190ull,7935135702156687355ull,3033127046167579202ull,14224256960933395097ull,10838403249753694181ull,2154089102842257532ull,7860358918492191001ull,2982010253383852617ull,16385171982396620123ull,12241857497176342828ull,2080931105225959532ull,1046322072991155713ull,6146917059052005252ull,17411786298437646544ull,5497869583209795613ull,11701448129764809247ull,12642962700918363620ull,15936842187305218463ull,7811510447588439153ull,3558405966224377785ull,977960926168429540ull,9505800334935014018ull,12114068456102275321ull,5141880021314950000ull,6719615890604904521ull,1341445859098821585ull,3883912906202435997ull,2107770591867486616ull,2657186337437393032ull,2640917573672927653ull,3746140861437224253ull,15057648507099656234ull,12051189681068107042ull,2259769676757597701ull,2935229535510718769ull,6368233316971463582ull,14384644474340782197ull,2553547617837260603ull,14238122466576902747ull,9555765226032904481ull,15522640015319979866ull,10274396157562093026ull,5996101113505388770ull,16915812546351047056ull,4956089714130804219ull,17126605744801075545ull,12036643325202409080ull,11257234688654558199ull,375338337104024778ull,11152980243617851986ull,12325805905403174063ull,8653948654121626815ull,15348912598299408338ull,6883296938248095081ull,6484642948886870833ull,16936141613107270500ull,17012171815528507292ull,2574129622316042070ull,17178726110735453748ull,16578303277501346489ull}),
                         createColumn<UInt64>({0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}));
    // clang-format on
}
CATCH

} // namespace tests
} // namespace DB
