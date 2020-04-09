#include <Interpreters/IDAsPathUpgrader.h>
#include <test_utils/TiflashTestBasic.h>

namespace DB::tests
{

TEST(IDAsPathUpgrader_test, test)
{
    TiFlashTestEnv::setupLogger();
    auto ctx = TiFlashTestEnv::getContext();
    IDAsPathUpgrader upgrader(ctx);
    ASSERT_TRUE(upgrader.needUpgrade());
    upgrader.doUpgrade();
}

} // namespace DB::tests
