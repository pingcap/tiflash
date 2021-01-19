#include <Storages/DeltaMerge/Range.h>
#include <test_utils/TiflashTestBasic.h>

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
