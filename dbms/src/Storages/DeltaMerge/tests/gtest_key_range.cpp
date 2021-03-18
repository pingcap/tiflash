#include <Storages/DeltaMerge/Range.h>
<<<<<<< HEAD
#include <test_utils/TiflashTestBasic.h>
=======
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <TestUtils/TiFlashTestBasic.h>
>>>>>>> 8f0b7ef1e... Refactor TiFlashRaftConfig / Define main entry point for `gtests_dbms` (#1583)

namespace DB
{
namespace DM
{
namespace tests
{

TEST(HandleRange_test, Redact)
{
    HandleRange range(20, 400);

    Redact::setRedactLog(false);
    EXPECT_EQ(range.toDebugString(), "[20,400)");

    Redact::setRedactLog(true);
    EXPECT_EQ(range.toDebugString(), "[?,?)");

    Redact::setRedactLog(false); // restore flags
}


} // namespace tests
} // namespace DM
} // namespace DB
