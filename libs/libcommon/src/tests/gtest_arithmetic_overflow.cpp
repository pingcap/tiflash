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

#include <common/arithmeticOverflow.h>
#include <common/types.h>
#include <gtest/gtest.h>
TEST(OVERFLOW_Suite, SimpleTest)
{
    /// mul int128
    __int128 res128;
    bool is_overflow;
    /// 2^126
    static constexpr __int128 int_126 = __int128(__int128(1) << 126);

    /// 2^126 << 0 = 2^126
    is_overflow = common::mulOverflow(int_126, __int128(1), res128);
    ASSERT_EQ(is_overflow, false);

    /// 2^126 << 1 = 2^127
    is_overflow = common::mulOverflow(int_126, __int128(2), res128);
    ASSERT_EQ(is_overflow, true);

    /// 2^126 << 2 = 2^128
    is_overflow = common::mulOverflow(int_126, __int128(4), res128);
    ASSERT_EQ(is_overflow, true);

    /// mul int256
    Int256 res256;
    /// 2^254
    static constexpr Int256 int_254 = Int256((Int256(0x1) << 254));
    /// 2^254 << 0 = 2^254
    is_overflow = common::mulOverflow(int_254, Int256(1), res256);
    ASSERT_EQ(is_overflow, false);

    /// 2^254 << 1 = 2^255
    is_overflow = common::mulOverflow(int_254, Int256(2), res256);
    ASSERT_EQ(
        is_overflow,
        false); /// because the sign flag is processed by an extra bit, excluding from 256 bits of Int256.

    /// 2^254 << 2 = 2^256
    is_overflow = common::mulOverflow(int_254, Int256(4), res256);
    ASSERT_EQ(is_overflow, true);
}
