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
class TestFunctionBitShiftLeft : public DB::tests::FunctionTest
{
};

#define ASSERT_BITSHIFTLEFT(t1, t2, result) ASSERT_COLUMN_EQ(result, executeFunction("bitShiftLeft", {t1, t2}))

TEST_F(TestFunctionBitShiftLeft, Simple)
try
{
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int64>>({11}),
        createColumn<Nullable<Int64>>({3}),
        createColumn<Nullable<UInt64>>({88}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitShiftLeft, others will be casted by TiDB planner.
/// Note: BitShiftLeft will further cast other types to UInt64 before doing shift.
TEST_F(TestFunctionBitShiftLeft, TypePromotion)
try
{
    // Type Promotion
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({-1}),
        createColumn<Nullable<Int16>>({1}),
        createColumn<Nullable<UInt64>>({18446744073709551614ull}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int16>>({-1}),
        createColumn<Nullable<Int32>>({1}),
        createColumn<Nullable<UInt64>>({18446744073709551614ull}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int32>>({-1}),
        createColumn<Nullable<Int64>>({1}),
        createColumn<Nullable<UInt64>>({18446744073709551614ull}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({-1}),
        createColumn<Nullable<Int64>>({1}),
        createColumn<Nullable<UInt64>>({18446744073709551614ull}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt16>>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    // Type Promotion across signed/unsigned
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int16>>({-1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({18446744073709551615ull}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int64>>({-1}),
        createColumn<Nullable<UInt8>>({0}),
        createColumn<Nullable<UInt64>>({18446744073709551615ull}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitShiftLeft, Nullable)
try
{
    // Non Nullable
    ASSERT_BITSHIFTLEFT(createColumn<Int8>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<Int16>({1}), createColumn<Int32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<Int32>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<Int8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITSHIFTLEFT(createColumn<UInt8>({1}), createColumn<UInt16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt32>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt8>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({1}));

    ASSERT_BITSHIFTLEFT(createColumn<Int16>({1}), createColumn<UInt32>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<Int64>({1}), createColumn<UInt8>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt32>({1}), createColumn<Int16>({0}), createColumn<UInt64>({1}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt8>({1}), createColumn<Int64>({0}), createColumn<UInt64>({1}));

    // Across Nullable and non-Nullable
    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int16>({1}),
        createColumn<Nullable<Int32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int32>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTLEFT(
        createColumn<UInt8>({1}),
        createColumn<Nullable<UInt16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<UInt16>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<UInt32>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<UInt8>({1}),
        createColumn<Nullable<UInt64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Int16>({1}),
        createColumn<Nullable<UInt32>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int64>({1}),
        createColumn<Nullable<UInt8>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<UInt32>({1}),
        createColumn<Nullable<Int16>>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<UInt8>({1}),
        createColumn<Nullable<Int64>>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Int16>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int16>>({1}),
        createColumn<Int32>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int32>>({1}),
        createColumn<Int64>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({1}),
        createColumn<Int64>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<UInt16>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt16>>({1}),
        createColumn<UInt32>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<UInt64>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<UInt64>({0}),
        createColumn<Nullable<UInt64>>({1}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int16>>({1}),
        createColumn<UInt32>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int64>>({1}),
        createColumn<UInt8>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt32>>({1}),
        createColumn<Int16>({0}),
        createColumn<Nullable<UInt64>>({1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt8>>({1}),
        createColumn<Int64>({0}),
        createColumn<Nullable<UInt64>>({1}));
}
CATCH

TEST_F(TestFunctionBitShiftLeft, TypeCastWithConst)
try
{
    /// need test these kinds of columns:
    /// 1. ColumnVector
    /// 2. ColumnVector<Nullable>
    /// 3. ColumnConst
    /// 4. ColumnConst<Nullable>, value != null
    /// 5. ColumnConst<Nullable>, value = null

    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({0, 0, 1, 2}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({0, 0, 1, 1}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<UInt64>(4, 0),
        createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createColumn<UInt64>({0, 0, 1, 1}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({0, 0, 1, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt)); // become const in wrapInNullable

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<Nullable<UInt64>>({0, 2, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, 2, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<UInt64>(4, 0),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITSHIFTLEFT(
        createConstColumn<Int8>(4, 1),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({1, 2, 1, 2}));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Int8>(4, 1),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({1, 2, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Int8>(4, 1),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Int8>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Int8>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createColumn<UInt64>({0, 1, 0, 1}),
        createColumn<UInt64>({1, 2, 1, 2}));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createColumn<Nullable<UInt64>>({1, 2, std::nullopt, std::nullopt}));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, 0),
        createConstColumn<UInt64>(4, 1));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, 1),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));

    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<UInt64>({0, 1, 0, 1}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<UInt64>(4, 0),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
    ASSERT_BITSHIFTLEFT(
        createConstColumn<Nullable<Int8>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt),
        createConstColumn<Nullable<UInt64>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitShiftLeft, Boundary)
try
{
    ASSERT_BITSHIFTLEFT(
        createColumn<Int8>({127, 127, -128, -128}),
        createColumn<UInt8>({0, 7, 0, 7}),
        createColumn<UInt64>({127, 16256, 18446744073709551488ull, 18446744073709535232ull}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int16>({32767, 32767, -32768, -32768}),
        createColumn<UInt8>({0, 15, 0, 15}),
        createColumn<UInt64>({32767, 1073709056, 18446744073709518848ull, 18446744072635809792ull}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
        createColumn<UInt64>({0, 63, 0, 63, 0, 63, 0, 63, 0, 63}),
        createColumn<UInt64>(
            {0,
             0,
             1,
             9223372036854775808ull,
             18446744073709551615ull,
             9223372036854775808ull,
             9223372036854775807ull,
             9223372036854775808ull,
             9223372036854775808ull,
             0}));
}
CATCH

TEST_F(TestFunctionBitShiftLeft, UINT64)
try
{
    ASSERT_BITSHIFTLEFT(
        createColumn<UInt64>({0, UINT64_MAX}),
        createColumn<UInt64>({63, 63}),
        createColumn<UInt64>({0, 9223372036854775808ull}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, std::nullopt}),
        createColumn<Nullable<UInt64>>({63, 63, 63}),
        createColumn<Nullable<UInt64>>({0, 9223372036854775808ull, std::nullopt}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, std::nullopt}),
        createColumn<UInt64>({63, 63, 63}),
        createColumn<Nullable<UInt64>>({0, 9223372036854775808ull, std::nullopt}));

    ASSERT_BITSHIFTLEFT(
        createColumn<UInt64>({0, UINT64_MAX}),
        createColumn<Nullable<UInt64>>({63, 63}),
        createColumn<Nullable<UInt64>>({0, 9223372036854775808ull}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, 0, 1, 0, 18446744073709551615ull, 0, INT64_MAX, 0, 9223372036854775808ull, 0}));


    ASSERT_BITSHIFTLEFT(
        createColumn<UInt64>({0, 0, UINT64_MAX, UINT64_MAX}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX}),
        createColumn<UInt64>({0, 0, UINT64_MAX, 0}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}),
        createColumn<Nullable<UInt64>>({0, UINT64_MAX, 0, UINT64_MAX, std::nullopt, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, 0, std::nullopt, std::nullopt}));

    ASSERT_BITSHIFTLEFT(
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
        createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
        createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, 0, std::nullopt}));

    ASSERT_BITSHIFTLEFT(
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
        res[i] = v1[i] << v2[i];
    }
    */
    // clang-format off
    ASSERT_BITSHIFTLEFT(createColumn<UInt64>({9590146258092595307ull,10707019645721939841ull,5881591201215258519ull,5632031715512392749ull,14458961951991665960ull,5128508001433333971ull,9332472261576879778ull,3894254697792674671ull,3686723867265895770ull,5228369181925377045ull,15257679458410288384ull,11771509639552105900ull,18418679795092108085ull,9855659417571021370ull,4077869334292632100ull,5391460931979150774ull,9467385864486395009ull,7668968327203373028ull,3000081540418664818ull,2526651933183909841ull,10848278664633919825ull,12728734861754908523ull,13066785977192899361ull,18052975499614771372ull,17129542673283010420ull,11328216476757777123ull,3472066935339592657ull,2176492510111902732ull,12990949065266798350ull,15009988423859086182ull,16923734737646560129ull,10145931322192369702ull,15411802497347660213ull,2061619294213261472ull,17580364414153312329ull,12125264059890046972ull,14105236895418755204ull,18398730584471114130ull,299685962966216102ull,16198499948406097599ull,2471494867787664413ull,4570374989605017140ull,6824988039821274928ull,9677834555477405663ull,4412087854764460966ull,4057574216689522200ull,4469278412271442169ull,2195483625075429663ull,11881108446786181585ull,5924332126027566084ull,3250541536484026531ull,14841325794537432635ull,8914756506150941362ull,2386255466980056883ull,13760701820714978003ull,8972642741869197165ull,16342958971153533843ull,6661808439105638065ull,13203982733978217950ull,12178802970326924901ull,12334199694527701475ull,17567161341866858140ull,12414652975269657715ull,2934467071243815081ull,6925619887984457268ull,16075344167916704125ull,9771482348696948711ull,7565184903186541823ull,1052089026349370803ull,1166166123827253256ull,14765270110884348174ull,1006637556365116527ull,4456932086646133707ull,14569355292824480150ull,644090626559619706ull,6739333562626065749ull,13046051594097911684ull,1889670651667918016ull,809698145438197192ull,1008561797702510136ull,15735380769470049759ull,13969312851421751236ull,5652663870952895520ull,6073389077452560954ull,17203692740201015991ull,12627416461571881794ull,17002372509101573427ull,12750188156795333967ull,4802871164855562574ull,5798598266310893099ull,11109002639116226418ull,11683464915217468028ull,2720046121015539944ull,12315139438585481078ull,12493077138920487022ull,2413460630123408726ull,2090136026867714615ull,9216644480497846345ull,16828372569894418887ull,2320332815156167319ull,}),
                         createColumn<UInt64>({24,54,57,48,35,51,39,15,49,0,52,6,60,11,21,15,7,31,42,34,53,23,25,7,11,46,27,42,48,25,23,22,19,46,61,49,17,35,24,36,21,32,56,49,46,19,16,12,5,26,28,58,56,45,25,2,47,25,24,36,19,43,3,30,20,26,56,63,59,61,24,24,50,33,8,60,31,1,37,51,43,54,51,22,30,12,9,12,51,31,60,27,32,17,4,54,57,0,35,38}),
                         createColumn<UInt64>({3891574726585221120ull,16158915463005339648ull,3314649325744685056ull,11253651043868737536ull,14614366883160260608ull,9698501797542363136ull,13616441058819833856ull,10809179421395091456ull,1924162940794044416ull,5228369181925377045ull,5764607523034234880ull,15506853982952712960ull,5764607523034234880ull,3652470547202297856ull,1277578713864601600ull,2923825176436736000ull,12787025863137706112ull,16453584519119765504ull,8207185798278152192ull,10514919160037769216ull,3035426148847714304ull,8458186846902419456ull,4259638639887122432ull,4937854736996783616ull,14042910761747718144ull,17778170635277565952ull,15774523305408593920ull,1346629065141911552ull,11461098101704491008ull,16423978333718446080ull,9943717430914187264ull,12830106927325249536ull,14247866488892948480ull,1308295691751129088ull,2305843009213693952ull,3456512714006855680ull,13579056934690488320ull,3179695887026749440ull,14674633785438371840ull,7988378517584740352ull,8042116021024194560ull,10501433581203619840ull,3458764513820540928ull,6898951679178178560ull,4821525613565181952ull,3604107309594181632ull,1227624260973428736ull,9136564412408262656ull,11260588822966778400ull,13293846752680476672ull,16329074277007491072ull,17005592192950992896ull,12826251738751172608ull,14116075635155664896ull,14706384829390258176ull,17443826893767237044ull,16557906242492694528ull,17693849764497457152ull,6250760634544160768ull,4390981392981295104ull,733708992845971456ull,10859550892101730304ull,7083503433609503640ull,4513840660133969920ull,16826445582531821568ull,7371066879428788224ull,16645304222761353216ull,9223372036854775808ull,10952754293765046272ull,0ull,17267913956513546240ull,9668302355338100736ull,5704934827971575808ull,16610774249069608960ull,17313247809586231808ull,5764607523034234880ull,16559722369413808128ull,3779341303335836032ull,14149387538942394368ull,3584865303386914816ull,3706172222256185344ull,17365880163140632576ull,11601272640106397696ull,14700896002525626368ull,8874759427379429376ull,15674187990554648576ull,16798265942806783488ull,2038217561947303936ull,9975473174625648640ull,5860325600132071424ull,2305843009213693952ull,9406390505584984064ull,2292183673182617600ull,6063068395571249152ull,15421793485632276192ull,6160924290242838528ull,7926335344172072960ull,9216644480497846345ull,14114226497115914240ull,301923419086127104ull,
}));
    // clang-format on
}
CATCH

TEST_F(TestFunctionBitShiftLeft, UB)
try
{
    ASSERT_BITSHIFTLEFT(createColumn<Int8>({127, -128}), createColumn<UInt8>({64, 64}), createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTLEFT(createColumn<Int8>({127, -128}), createColumn<UInt16>({64, 64}), createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int16>({32767, -32768}),
        createColumn<UInt8>({64, 64}),
        createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int32>({INT32_MAX, INT32_MIN}),
        createColumn<UInt8>({64, 64}),
        createColumn<UInt64>({0, 0}));
    ASSERT_BITSHIFTLEFT(
        createColumn<Int64>({INT64_MAX, INT64_MIN}),
        createColumn<UInt8>({64, 64}),
        createColumn<UInt64>({0, 0}));

    ASSERT_BITSHIFTLEFT(createColumn<UInt8>({255}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt8>({255}), createColumn<UInt16>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt16>({65535}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt32>({UINT32_MAX}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));
    ASSERT_BITSHIFTLEFT(createColumn<UInt64>({UINT64_MAX}), createColumn<UInt8>({64}), createColumn<UInt64>({0}));

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
    ASSERT_BITSHIFTLEFT(createColumn<UInt64>({17563387625296433369ull,5842891814427459261ull,15074502074821508463ull,386435802999553003ull,5487893274931198395ull,8125923807366590570ull,13340330062727071249ull,14908193031091561411ull,296805448857369387ull,8684453485792353774ull,13117933444495098288ull,3225762988982100714ull,11290506757949810556ull,14617912756126856962ull,9479575714707174581ull,11720728318194739598ull,14410575429605211363ull,12068356718035872518ull,80682389916710599ull,11003236134534292734ull,4412447398096224810ull,5331184707993902906ull,13827083432789678788ull,958142831027309576ull,16716461997317184701ull,17128750834581527743ull,11590434571174666313ull,10204342520615148287ull,11067791415848657283ull,17583875436196878829ull,186304014359496415ull,9381729025189804702ull,11502205568225715300ull,16472133582690439104ull,3743303387826342067ull,12860029445868505658ull,2244056593742923769ull,3275687468466891223ull,1545828456957460699ull,14187252460708728077ull,7551907967738536187ull,9754400233340010491ull,16293183350230169116ull,6298812696728711031ull,5915538565572009956ull,2284684518775825662ull,1130711226902262476ull,17158957721471765323ull,4220824385439711070ull,16559772875254313109ull,15397179690017513678ull,6300413832999049491ull,13787530251307637715ull,10132349060092695582ull,10446586881482901699ull,15759779838283537085ull,14402587207027333363ull,5546051719872960161ull,6545031029710296628ull,17407295406267098658ull,4259019625544816073ull,791895457880289787ull,8549227257401578066ull,15246278171168501125ull,1674668228908076954ull,849762797502000057ull,13302651500925764574ull,12438174880334092333ull,17701249772557033303ull,10742459186038873636ull,15671491258945407856ull,9352557101631889001ull,8914093883925002585ull,17935292744735591949ull,606989231583658922ull,6528503454270721815ull,14980539549624989095ull,13765196438235456668ull,3058323869228644592ull,14346577759191739044ull,1543206286382906519ull,1025562312317433790ull,17052896445025268012ull,18349597294988935754ull,17174604730104962524ull,11924965352621110201ull,502032511104181724ull,13845633389643139332ull,15436039204445155412ull,17809579006694175565ull,15166364145138562881ull,14062748599121933798ull,1777457178576774356ull,4985224560472716170ull,3881603168175384251ull,11555031280550342082ull,1252677486917153396ull,8744807353133366467ull,2048964426549800495ull,11945831330508218140ull}),
                         createColumn<UInt64>({10170168382087373376ull,13942906057510103279ull,14306855544784320750ull,14490595809993733085ull,13894994773265190130ull,4539744309919296456ull,5960451177866905037ull,8254484395144502863ull,15947787413795976654ull,11758211605027987396ull,1969488148300388656ull,3690019359734755802ull,2761764580773985855ull,8214602624640213083ull,10104065697094616841ull,12719638633135031120ull,1579356563729340757ull,9849275185119527902ull,13259386372230333703ull,11525029819550436195ull,9336811033678812919ull,18024494470684769699ull,16262431544264631528ull,747508930926384196ull,6852408932132380275ull,16193626994293308072ull,3668439249087146688ull,2815114051955836547ull,11623595120239653162ull,14051110314333912266ull,3470211856726399766ull,24326123962172569ull,8671257608652446190ull,4449812597097007527ull,420576121975887020ull,5947488386148607688ull,7703644645840795485ull,6034247569925525574ull,7207924845921854255ull,1628903707897208595ull,17386978449329440960ull,11483226896151393705ull,5716613009851345329ull,16637909040452729752ull,8923037827908078416ull,9873656643203662744ull,10728065007959141271ull,9289790999990424278ull,4984880433949807755ull,4081441263589415253ull,14141469082911534070ull,13537500414106209761ull,3771459446515323468ull,3803220448332221997ull,5872361935348309433ull,16931084214526286271ull,229523845509607962ull,6137782246193543039ull,9416772409169847829ull,1599362648203361205ull,16506466156139756326ull,5594658406006721749ull,6075086402035041023ull,8193007286917228777ull,10417397014019249797ull,8027438655725876281ull,3744210550662242051ull,4486285497361262297ull,5233531540019046791ull,8754524731116108320ull,11747423804646205366ull,10823676266667258725ull,2377832589519205884ull,10149462695348926053ull,9938241660263331809ull,1691565986446695935ull,12741697332550437523ull,14377762655871009378ull,2710283096015101134ull,1991666937410026062ull,16045620270500586077ull,3635648749144116339ull,3398892026331619397ull,13943407331484180936ull,6636826897898447964ull,2810976231716911209ull,12715335843259733155ull,9407059307990209078ull,4918637361023593506ull,13248043003208654795ull,8205307620677927795ull,2432590649498729202ull,12261496797882837416ull,12870696446667604684ull,13094194364612141901ull,16877489047893851037ull,3133779556474902761ull,10042552922284313547ull,2121324263442996583ull,15840313846181544148ull}),
                         createColumn<UInt64>({0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}));
    // clang-format on
}
CATCH

} // namespace tests
} // namespace DB
