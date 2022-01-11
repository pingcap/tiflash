#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeNothing.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
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

#define ASSERT_BITAND(t1, t2, result) \
    ASSERT_COLUMN_EQ(result, executeFunction("bitAnd", {t1, t2}))

TEST_F(TestFunctionBitAnd, Simple)
try
{
    ASSERT_BITAND(createColumn<Nullable<Int64>>({-1, 1}), createColumn<Nullable<Int64>>({0, 0}), createColumn<Nullable<Int64>>({0, 0}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitAnd, others will be casted by TiDB Planner.
TEST_F(TestFunctionBitAnd, TypePromotion)
try
{
    // Type Promotion
    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<Nullable<Int32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int32>>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<UInt16>>({0}), createColumn<Nullable<UInt16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt16>>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));

    // Type Promotion across signed/unsigned
    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int64>>({1}), createColumn<Nullable<UInt8>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));
}
CATCH

TEST_F(TestFunctionBitAnd, Nullable)
try
{
    // Non Nullable
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Int16>({0}), createColumn<Int16>({0}));
    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Int32>({0}), createColumn<Int32>({0}));
    ASSERT_BITAND(createColumn<Int32>({1}), createColumn<Int64>({0}), createColumn<Int64>({0}));
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Int64>({0}), createColumn<Int64>({0}));

    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<UInt16>({0}), createColumn<UInt16>({0}));
    ASSERT_BITAND(createColumn<UInt16>({1}), createColumn<UInt32>({0}), createColumn<UInt32>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({0}));

    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<UInt32>({0}), createColumn<Int32>({0}));
    ASSERT_BITAND(createColumn<Int64>({1}), createColumn<UInt8>({0}), createColumn<Int64>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Int16>({0}), createColumn<Int32>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Int64>({0}), createColumn<Int64>({0}));

    // Across Nullable and non-Nullable
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int16>>({0}));
    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Nullable<Int32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Int32>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<UInt16>>({0}), createColumn<Nullable<UInt16>>({0}));
    ASSERT_BITAND(createColumn<UInt16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt32>>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Int64>({1}), createColumn<Nullable<UInt8>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Int16>({0}), createColumn<Nullable<Int16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<Int32>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int32>>({1}), createColumn<Int64>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<UInt16>({0}), createColumn<Nullable<UInt16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int64>>({1}), createColumn<UInt8>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<Int16>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<Int64>>({0}));
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

    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Int64>({0, 0, 0, 1}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createConstColumn<UInt64>(4, 0), createColumn<Int64>({0, 0, 0, 0}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createConstColumn<Nullable<UInt64>>(4, 0), createColumn<Nullable<Int64>>({0, 0, 0, 0}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt)); // become const in wrapInNullable

    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Nullable<Int64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<UInt64>(4, 0), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<Nullable<UInt64>>(4, 0), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));

    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Int64>({0, 0, 0, 0}));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createConstColumn<UInt64>(4, 0), createConstColumn<Int64>(4, 0));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createConstColumn<Nullable<UInt64>>(4, 0), createConstColumn<Nullable<Int64>>(4, 0));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));

    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Nullable<Int64>>({0, 0, 0, 0}));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createConstColumn<UInt64>(4, 0), createConstColumn<Nullable<Int64>>(4, 0));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createConstColumn<Nullable<UInt64>>(4, 0), createConstColumn<Nullable<Int64>>(4, 0));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));

    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createColumn<UInt64>({0, 1, 0, 1}), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createConstColumn<UInt64>(4, 0), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createConstColumn<Nullable<UInt64>>(4, 0), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitAnd, Boundary)
try
{
    ASSERT_BITAND(createColumn<Int8>({127, 127, -128, -128}), createColumn<UInt8>({0, 255, 0, 255}), createColumn<Int8>({0, 127, 0, -128}));
    ASSERT_BITAND(createColumn<Int8>({127, 127, -128, -128}), createColumn<UInt16>({0, 65535, 0, 65535}), createColumn<Int16>({0, 127, 0, -128}));
    ASSERT_BITAND(createColumn<Int16>({32767, 32767, -32768, -32768}), createColumn<UInt8>({0, 255, 0, 255}), createColumn<Int16>({0, 255, 0, 0}));

    ASSERT_BITAND(createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
                  createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX}),
                  createColumn<Int64>({0, 0, 0, 1, 0, -1, 0, INT64_MAX, 0, INT64_MIN}));
}
CATCH

TEST_F(TestFunctionBitAnd, UINT64)
try
{
    ASSERT_BITAND(createColumn<UInt64>({0, 0, UINT64_MAX, UINT64_MAX}),
                  createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX}),
                  createColumn<UInt64>({0, 0, 0, UINT64_MAX}));

    ASSERT_BITAND(createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, 0, std::nullopt}),
                  createColumn<Nullable<UInt64>>({0, UINT64_MAX, 0, UINT64_MAX, std::nullopt, 0}),
                  createColumn<Nullable<UInt64>>({0, 0, 0, UINT64_MAX, std::nullopt, std::nullopt}));

    ASSERT_BITAND(createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
                  createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
                  createColumn<Nullable<UInt64>>({0, 0, 0, UINT64_MAX, std::nullopt}));

    ASSERT_BITAND(createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0}),
                  createColumn<Nullable<UInt64>>({0, 0, UINT64_MAX, UINT64_MAX, std::nullopt}),
                  createColumn<Nullable<UInt64>>({0, 0, 0, UINT64_MAX, std::nullopt}));

    /*
    size_t count = 100;
    std::vector<UINT64> v1(count), v2(count), res(count);
    for (size_t i=0; i<count; ++i) {
        std::mt19937 gen(std::random_device{}());
        std::uniform_int_distribution<unsigned long long> dis(
                std::numeric_limits<std::uint64_t>::min(),
                std::numeric_limits<std::uint64_t>::max()
        );
        v1[i] = dis(gen);
        v2[i] = dis(gen);
        res[i] = v1[i] & v2[i];
    }
    */
    ASSERT_BITAND(createColumn<UInt64>({10598503734948058908ull, 11233337816902033995ull, 6174966050735739257ull, 7876144582541867009ull, 920845522613386304ull, 5372106575617648570ull, 16897901033330576622ull, 5296909135541736546ull, 15409128204318894600ull, 7234014726253714937ull, 5909513227437183168ull, 783943482162592804ull, 12168726180061857466ull, 8708024385141534655ull, 8095203096768508365ull, 14595057649891224239ull, 7501229575892830372ull, 15625320150767851655ull, 8403777588635538393ull, 3954803434638120804ull, 3107819797935406000ull, 10620045064243203295ull, 3570382741413038059ull, 16115499137953863267ull, 6762341073785158799ull, 14826818944586771185ull, 16871799952300726730ull, 10576632631323435803ull, 11246015453388342627ull, 13455030513459976524ull, 733199058235283499ull, 15938097154704182515ull, 1237419733290339132ull, 4493348543454192477ull, 7032764718045447267ull, 16050759521496924354ull, 17270776879892706263ull, 12609541003237003312ull, 11134965005023828567ull, 15231860720823570499ull, 5836027220679157494ull, 17822882453499239286ull, 15785090323381225155ull, 5979870360435555989ull, 17984543211302890692ull, 9878531603097522091ull, 3557506460608513407ull, 1582897106985507915ull, 12838446251550139345ull, 181434117670616337ull, 11040780551467848122ull, 14134416534354148642ull, 10998848181891400524ull, 6924169996241615209ull, 1302721064386749844ull, 4121463993214323132ull, 3485864567999448120ull, 14625497932278087830ull, 5511536340563484645ull, 2336868243397576229ull, 550190771078490427ull, 18137048967398341803ull, 11284782365919797978ull, 12987619858343256367ull, 2058975012832968980ull, 2152488373032173646ull, 12209252339448585596ull, 403680720305437455ull, 8448808844293935546ull, 2932904676939280838ull, 11223742974586999119ull, 11716158319378004369ull, 16455547689825239378ull, 4264844999426220566ull, 13450232913651642506ull, 8605843370166236935ull, 7098578139950353605ull, 3070093331286443731ull, 12701037583226079871ull, 3198812648954467065ull, 563673760622735494ull, 15646172537481613298ull, 1503342518489059259ull, 4277360076116677838ull, 5856247146354221442ull, 455788235636388917ull, 1153962694540524928ull, 10971201452227181238ull, 4932845825016803415ull, 4125926937174158241ull, 15418287531941107903ull, 9450290325125542004ull, 8088789518985183890ull, 8734825483173939188ull, 6130890481549162236ull, 11075825338369629185ull, 17590581157171849321ull, 14421987733222947912ull, 6593467235817427122ull, 15800418434231593441ull,
                                        }),
                  createColumn<UInt64>({14217713913424424483ull,1545092628052442720ull,5199434064608884358ull,8177688168726450368ull,17804225606936231190ull,3375543070307400512ull,18356948288724879759ull,5476764770372649864ull,4058263737105323755ull,10453935585680790119ull,6049672786202495239ull,11138318035641135890ull,13463138185608411844ull,5666843897121014805ull,18004398642659848128ull,7686663921212224574ull,9519761693798619498ull,3356409076979074150ull,15347571558376471992ull,12035155289896957574ull,11073898149828537595ull,10707100248015452603ull,12967204572672048713ull,2837612405438078176ull,1065389286637572939ull,247012257965768948ull,5500678584186120928ull,18209967366303868307ull,9840428258328391468ull,7490302969799569724ull,9330293201160865732ull,12398871096176820767ull,11075080354682784952ull,55028875486847076ull,10410669121770770212ull,1333701017121307770ull,14503516862325560312ull,13849692947509982105ull,379528414307110895ull,5095114254883129336ull,3128747703077707593ull,11644110948578271891ull,246382241677659695ull,5322388953803039050ull,14829967970096971073ull,5463973807061028694ull,14288887982025488710ull,15537917678856486330ull,1430263509954286284ull,895139333373264112ull,13290845228837709751ull,6518749841745407906ull,4571588734283785216ull,16041711093613958580ull,4890397069539590426ull,15183451233194672872ull,2544593499127131832ull,3683987560372985739ull,7772481859392360245ull,553758540259856146ull,1849529746174548361ull,8327738946263349444ull,14124689739384555853ull,10701533504714418629ull,15570153625924465571ull,10271032799617358534ull,11087147301281619553ull,5176973038847716133ull,15094300982504124985ull,10442594770731332082ull,16170360226524823121ull,8517683548077241387ull,11156727365308389078ull,10729270848562651519ull,4987714452996405579ull,10841415386928888205ull,14419689215110601359ull,4057072316289111187ull,12042180081181971684ull,5664049404078130101ull,12105325792048297668ull,13719762962316338781ull,2149425063797595885ull,11217998099186939142ull,12362050582742734384ull,16798012105656723746ull,4049468774438919817ull,16836190449596436170ull,13622232309674921244ull,13550452781203765819ull,3547664695109194155ull,10947623482017681252ull,5018864594902719673ull,9755567607352977242ull,3582384154357065143ull,3497168457639628248ull,16318679780547167515ull,13873546223447514232ull,14048847424500608166ull,2186727430779353513ull,
                                        }),
                  createColumn<UInt64>({15519254782337023807ull,11526353839157149291ull,6753706175536297983ull,9042664895360727233ull,18435433260130106710ull,7988786505113862138ull,18357234438375034351ull,5585420988817840106ull,18291925689557024491ull,17687877743626002431ull,6050251817722288583ull,11165375094975364918ull,13465762877176545022ull,9150679520217264063ull,18005384161653161933ull,16910048613323734719ull,17014033001341770222ull,18364656621901929703ull,17653467554635251705ull,13251698957345208294ull,13524155422881071099ull,10950496961932787199ull,12969469724533211115ull,18439418542592548579ull,6906742723445751759ull,14983459842175789813ull,17183201457971100650ull,18372434508059240347ull,11282079457095266159ull,18445617073736841596ull,10051995591848746991ull,18248585672522649343ull,11078493309643411388ull,4529377651711501181ull,17436643082236841831ull,16050761721595280634ull,17289028420806489087ull,17221763858217743289ull,11513276242027208703ull,15562043376793458683ull,8935066858174251007ull,17861198331426896887ull,15812133171739840239ull,6628433258527195103ull,18293603870810570181ull,14688489359632060415ull,17824930596929568127ull,15562142120119500283ull,12969051764347597789ull,1075442765937360369ull,13365200983242143679ull,16030502346762911650ull,13831622904573306700ull,18347559328764902909ull,6043338118398272926ull,18137814273787382780ull,3706593744301059768ull,18156323384311242655ull,8069869009748615157ull,2877652266832072503ull,2281912788766226875ull,18137051175012728047ull,15897702286281927647ull,13024268800726133231ull,15895172308226864055ull,11519976871957486286ull,13402712469478236029ull,5177571659044551471ull,17688374505593762747ull,13329473883952050678ull,18152531220341194591ull,17779340442481425851ull,18365109174202701782ull,13830492882465186687ull,18428725135285402059ull,17833825696321486735ull,16906293053696433871ull,4242318139062212307ull,13213401859788889855ull,7998111394446119933ull,12105345876154250950ull,18403537389361131519ull,2151870421144313855ull,13546403095732254158ull,18144954256021436338ull,17248733791706021175ull,4049786055618461577ull,18007550303570050814ull,18266597052004040031ull,13641119266517007291ull,17724997146973804991ull,10948230902348234612ull,8495961489767128763ull,18409305150806878206ull,8482370919904311295ull,13383990552380300761ull,17762158301211880827ull,14460422812613966968ull,15850343900500147382ull,16095546051993074153ull,
                                        }));
}
CATCH


} // namespace tests
} // namespace DB
