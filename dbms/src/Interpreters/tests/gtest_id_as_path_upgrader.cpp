#include <Interpreters/IDAsPathUpgrader.h>
#include <test_utils/TiflashTestBasic.h>

namespace DB::tests
{

TEST(IDAsPathUpgrader_test, test)
try
{
    TiFlashTestEnv::setupLogger();
    auto ctx = TiFlashTestEnv::getContext();
    IDAsPathUpgrader upgrader(ctx);
    ASSERT_TRUE(upgrader.needUpgrade());
    upgrader.doUpgrade();

    {
        // After upgrade, next time we don't need it.
        IDAsPathUpgrader checker_after_upgrade(ctx);
        ASSERT_FALSE(checker_after_upgrade.needUpgrade());
    }
}
CATCH

} // namespace DB::tests
