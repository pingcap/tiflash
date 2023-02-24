#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3GCManager.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>
#include <TestUtils/TiFlashTestEnv.h>

namespace DB::S3
{

class S3GCManagerTest : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
    }
};

TEST_F(S3GCManagerTest, Simple)
try
{
    S3GCConfig config{
        .manifest_expired_hour = 1,
        .delmark_expired_hour = 1,
        .temp_path = tests::TiFlashTestEnv::getTemporaryPath(),
    };
    S3GCManager gc_mgr(config);
    gc_mgr.runOnAllStores();
}
CATCH

} // namespace DB::S3
