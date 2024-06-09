// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/RedactHelpers.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{

TEST(RedactLogTest, Basic)
{
    const char * test_key = "\x01\x0a\xff";
    const size_t key_sz = strlen(test_key);

    const /*DB::HandleID*/ Int64 test_handle = 10009;

    Redact::setRedactLog(RedactMode::Disable);
    EXPECT_EQ(Redact::keyToDebugString(test_key, key_sz), "010AFF");
    EXPECT_EQ(Redact::keyToHexString(test_key, key_sz), "010AFF");
    EXPECT_EQ(Redact::handleToDebugString(test_handle), "10009");

    Redact::setRedactLog(RedactMode::Marker);
    EXPECT_EQ(Redact::keyToDebugString(test_key, key_sz), "‹010AFF›");
    EXPECT_EQ(Redact::keyToHexString(test_key, key_sz), "010AFF");
    EXPECT_EQ(Redact::handleToDebugString(test_handle), "‹10009›");

    Redact::setRedactLog(RedactMode::Enable);
    EXPECT_EQ(Redact::keyToDebugString(test_key, key_sz), "?");
    EXPECT_EQ(Redact::keyToHexString(test_key, key_sz), "010AFF"); // Unaffected by readact-log status
    EXPECT_EQ(Redact::handleToDebugString(test_handle), "?");

    Redact::setRedactLog(RedactMode::Disable); // restore flags
}

} // namespace DB::tests
