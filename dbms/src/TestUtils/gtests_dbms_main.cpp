#include <Common/FailPoint.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::FailPoints
{
extern const char force_set_dtfile_exist_when_acquire_id[];
} // namespace DB::FailPoints

int main(int argc, char ** argv)
{
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();

#ifdef FIU_ENABLE
    fiu_init(0); // init failpoint

    DB::FailPointHelper::enableFailPoint(DB::FailPoints::force_set_dtfile_exist_when_acquire_id);
#endif

    ::testing::InitGoogleTest(&argc, argv);
    auto ret = RUN_ALL_TESTS();

    DB::tests::TiFlashTestEnv::shutdown();

    return ret;
}
