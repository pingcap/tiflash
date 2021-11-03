#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "region_helper.h"


namespace DB
{
namespace tests
{
TEST_F(RaftAdminCmd_test, mergeresult)
try
{
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "x", ""), createRegionInfo(1000, "", "x")).source_at_left, false);
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "", "x"), createRegionInfo(1000, "x", "")).source_at_left, true);
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "x", "y"), createRegionInfo(1000, "y", "z")).source_at_left, true);
    ASSERT_EQ(MetaRaftCommandDelegate::computeRegionMergeResult(createRegionInfo(1, "y", "z"), createRegionInfo(1000, "x", "y")).source_at_left, false);
}
CATCH

} // namespace tests

} // namespace DB
