#include <Interpreters/IDAsPathUpgrader.h>
#include <test_utils/TiflashTestBasic.h>
#include <Interpreters/loadMetadata.h>

namespace DB::tests
{

// This test need to prepare some directory in local
// Can not find a good way to support it in CI, ignore for now.
TEST(IDAsPathUpgrader_test, DISABLED_test)
try
{
    TiFlashTestEnv::setupLogger();
    auto ctx = TiFlashTestEnv::getContext();
    IDAsPathUpgrader upgrader(ctx, false);
    ASSERT_TRUE(upgrader.needUpgrade());
    upgrader.doUpgrade();

    {
        // After upgrade, next time we don't need it.
        IDAsPathUpgrader checker_after_upgrade(ctx, false);
        ASSERT_FALSE(checker_after_upgrade.needUpgrade());
    }

    loadMetadata(ctx);
}
CATCH

} // namespace DB::tests
