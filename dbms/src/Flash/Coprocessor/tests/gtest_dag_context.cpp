#include <Flash/Coprocessor/DAGContext.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
TEST(DAGContextTest, FlagsTest)
{
    DAGContext context(1024);

    ASSERT_EQ(context.getFlags(), static_cast<UInt64>(0));

    UInt64 f = TiDBSQLFlags::TRUNCATE_AS_WARNING | TiDBSQLFlags::IN_LOAD_DATA_STMT;
    context.setFlags(f);
    ASSERT_EQ(context.getFlags(), f);

    UInt64 f1 = f | TiDBSQLFlags::OVERFLOW_AS_WARNING;
    context.addFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);
    ASSERT_EQ(context.getFlags(), f1);

    context.delFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);
    ASSERT_EQ(context.getFlags(), f);
}

} // namespace tests
} // namespace DB
