#include <TestUtils/TiFlashTestBasic.h>
#include <Common/RedactHelpers.h>

namespace DB
{
namespace tests
{

TEST(RedactLog_test, Basic)
{
    const char * test_key = "\x01\x0a\xff";
    const size_t key_sz = strlen(test_key);

    const DB::HandleID test_handle = 10009;

    Redact::setRedactLog(false);
    EXPECT_EQ(Redact::keyToDebugString(test_key, key_sz), "010AFF");
    EXPECT_EQ(Redact::keyToHexString(test_key, key_sz), "010AFF");
    EXPECT_EQ(Redact::handleToDebugString(test_handle), "10009");

    Redact::setRedactLog(true);
    EXPECT_EQ(Redact::keyToDebugString(test_key, key_sz), "?");
    EXPECT_EQ(Redact::keyToHexString(test_key, key_sz), "010AFF"); // Unaffected by readact-log status
    EXPECT_EQ(Redact::handleToDebugString(test_handle), "?");

    Redact::setRedactLog(false); // restore flags
}

}
}
