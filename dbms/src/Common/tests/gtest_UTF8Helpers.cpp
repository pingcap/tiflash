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

#include <TestUtils/TiFlashTestBasic.h>

#include <Common/UTF8Helpers.cpp>

namespace DB
{
namespace tests
{
/// Reference https://github.com/skeeto/branchless-utf8/blob/master/test/tests.c

unsigned char * utf8_encode(unsigned char * s, long c)
{
    if (c >= (1L << 16))
    {
        s[0] = 0xf0 | (c >> 18);
        s[1] = 0x80 | ((c >> 12) & 0x3f);
        s[2] = 0x80 | ((c >> 6) & 0x3f);
        s[3] = 0x80 | ((c >> 0) & 0x3f);
        return s + 4;
    }
    else if (c >= (1L << 11))
    {
        s[0] = 0xe0 | (c >> 12);
        s[1] = 0x80 | ((c >> 6) & 0x3f);
        s[2] = 0x80 | ((c >> 0) & 0x3f);
        return s + 3;
    }
    else if (c >= (1L << 7))
    {
        s[0] = 0xc0 | (c >> 6);
        s[1] = 0x80 | ((c >> 0) & 0x3f);
        return s + 2;
    }
    else
    {
        s[0] = c;
        return s + 1;
    }
}

class TestUTF8Utility : public ::testing::Test
{
};

TEST_F(TestUTF8Utility, TestDecode)
try
{
    /* Make sure it can decode every character */
    {
        size_t failures = 0;
        for (size_t i = 0; i < UTF8::UNICODE_Max; ++i)
        {
            if (!IS_SURROGATE(i))
            {
                unsigned char buf[8] = {0};
                unsigned char * end = utf8_encode(buf, i);
                auto res = UTF8::utf8Decode(reinterpret_cast<char *>(buf), 8);
                failures += res.second != (end - buf) || res.first != i || res.first == UTF8::UTF8_Error;
                if (failures > 0)
                    break;
            }
        }
        ASSERT_TRUE(failures == 0);
    }

    /* Reject everything outside of U+0000..U+10FFFF */
    {
        size_t failures = 0;
        for (size_t i = 0x110000; i < 0x1fffff; ++i)
        {
            unsigned char buf[8] = {0};
            utf8_encode(buf, i);
            auto res = UTF8::utf8Decode(reinterpret_cast<char *>(buf), 8);
            failures += res.first != UTF8::UTF8_Error;
            failures += res.second != 1;
        }
        ASSERT_TRUE(failures == 0);
    }

    /* Does it reject all surrogate halves? */
    {
        size_t failures = 0;
        for (size_t i = 0xd800; i <= 0xdfff; ++i)
        {
            unsigned char buf[8] = {0};
            utf8_encode(buf, i);
            auto res = UTF8::utf8Decode(reinterpret_cast<char *>(buf), 8);
            failures += res.first != UTF8::UTF8_Error;
        }
        ASSERT_TRUE(failures == 0);
    }

    /* How about non-canonical encodings? */
    {
        unsigned char buf2[8] = {0xc0, 0xA4};
        auto res = UTF8::utf8Decode(reinterpret_cast<char *>(buf2), 8);
        ASSERT_TRUE(res.first == UTF8::UTF8_Error);

        unsigned char buf3[8] = {0xe0, 0x80, 0xA4};
        res = UTF8::utf8Decode(reinterpret_cast<char *>(buf3), 8);
        ASSERT_TRUE(res.first == UTF8::UTF8_Error);

        unsigned char buf4[8] = {0xf0, 0x80, 0x80, 0xA4};
        res = UTF8::utf8Decode(reinterpret_cast<char *>(buf4), 8);
        ASSERT_TRUE(res.first == UTF8::UTF8_Error);
    }

    /* Let's try some bogus byte sequences */
    {
        /* Invalid length */
        unsigned char buf[4] = {0xff};
        auto res = UTF8::utf8Decode(reinterpret_cast<char *>(buf), 0);
        ASSERT_TRUE(res.first == UTF8::UTF8_Error);
        ASSERT_TRUE(res.second == 0);

        /* Invalid first byte */
        unsigned char buf0[4] = {0xff};
        res = UTF8::utf8Decode(reinterpret_cast<char *>(buf0), 4);
        ASSERT_TRUE(res.first == UTF8::UTF8_Error);

        /* Invalid first byte */
        unsigned char buf1[4] = {0x80};
        res = UTF8::utf8Decode(reinterpret_cast<char *>(buf1), 4);
        ASSERT_TRUE(res.first == UTF8::UTF8_Error);

        /* Looks like a two-byte sequence but second byte is wrong */
        unsigned char buf2[4] = {0xc0, 0x0a};
        res = UTF8::utf8Decode(reinterpret_cast<char *>(buf2), 4);
        ASSERT_TRUE(res.first == UTF8::UTF8_Error);
    }
}
CATCH
} // namespace tests
} // namespace DB