#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
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
    EXPECT_EQ(range.toDebugString(), "[20, 400)");

    Redact::setRedactLog(true);
    EXPECT_EQ(range.toDebugString(), "[?, ?)");
}


TEST(RowKeyRange_test, Redact)
{
    RowKeyRange range = RowKeyRange::fromHandleRange(HandleRange{20, 400})

    Redact::setRedactLog(false);
    EXPECT_EQ(range.toDebugString(), "[20, 400)");

    Redact::setRedactLog(true);
    EXPECT_EQ(range.toDebugString(), "[?, ?)");
}

}
} // namespace DM
} // namespace DB
