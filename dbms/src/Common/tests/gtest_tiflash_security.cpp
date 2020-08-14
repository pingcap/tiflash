#include <Common/TiFlashSecurity.h>
#include <gtest/gtest.h>

#include <ext/singleton.h>

namespace DB
{
namespace tests
{

class TestTiFlashSecurity : public ext::singleton<TestTiFlashSecurity>
{
};

TEST(TestTiFlashSecurity, Config)
{
    TiFlashSecurityConfig config;
    config.parseAllowedCN(String("[abc,efg]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);

    config.allowed_common_names.clear();

    config.parseAllowedCN(String("[\"abc\",\"efg\"]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);

    config.allowed_common_names.clear();

    config.parseAllowedCN(String("[ abc , efg ]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);

    config.allowed_common_names.clear();

    config.parseAllowedCN(String("[ \"abc\", \"efg\" ]"));
    ASSERT_EQ((int)config.allowed_common_names.count("abc"), 1);
    ASSERT_EQ((int)config.allowed_common_names.count("efg"), 1);
}
} // namespace tests
} // namespace DB
