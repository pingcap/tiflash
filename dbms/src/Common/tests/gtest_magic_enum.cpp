// Copyright 2022 PingCAP, Ltd.
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

#include <common/crc64.h>
#include <gtest/gtest.h>

#include <magic_enum.hpp>
using namespace magic_enum;

namespace DB::tests
{
TEST(MagicEnumTest, APITest)
{
    using crc64::Mode;
    constexpr auto mode_entries = magic_enum::enum_entries<Mode>();
    for (const auto & entry : mode_entries)
    {
        // enum value to string
        ASSERT_EQ(magic_enum::enum_name(entry.first), entry.second);
        // string to enum value
        ASSERT_EQ(entry.first, magic_enum::enum_cast<Mode>(entry.second));
    }
}
} // namespace DB::tests
