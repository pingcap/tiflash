#include <Common/FailPoint.h>
#include <TestUtils/TiFlashTestBasic.h>

int main(int argc, char ** argv)
{
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();

#ifdef FIU_ENABLE
    fiu_init(0); // init failpoint
#endif

    ::testing::InitGoogleTest(&argc, argv);
    auto ret = RUN_ALL_TESTS();

    DB::tests::TiFlashTestEnv::shutdown();

    return ret;
}
