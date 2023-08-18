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

#include <common/crc64.h>
#include <gtest/gtest.h>

#include <magic_enum.hpp>

namespace DB::tests
{
TEST(MagicEnumTest, EnumConversion)
{
    using crc64::Mode;
    // mode_entries -> {{Mode::Table, "Table"}, {Mode::Auto, "Auto"}, {Mode::SIMD_128, "SIMD_128"}...}
    // mode_entries[0].first -> Mode::Table
    // mode_entries[0].second -> "Table"
    constexpr auto mode_entries = magic_enum::enum_entries<Mode>();
    ASSERT_EQ(mode_entries.size(), magic_enum::enum_names<Mode>().size());
    ASSERT_EQ(mode_entries.size(), magic_enum::enum_values<Mode>().size());
    ASSERT_EQ(mode_entries.size(), magic_enum::enum_count<Mode>());

    for (const auto & entry : mode_entries)
    {
        // enum value to string
        ASSERT_EQ(magic_enum::enum_name(entry.first), entry.second);
        // string to enum value
        auto mode = magic_enum::enum_cast<Mode>(entry.second);
        ASSERT_TRUE(mode.has_value());
        ASSERT_EQ(entry.first, mode);
    }

    // enum value to integer
    int mode_integer = 2;
    auto mode_from_int = magic_enum::enum_cast<Mode>(mode_integer);
    ASSERT_TRUE(mode_from_int.has_value());
    ASSERT_EQ(mode_from_int.value(), Mode::SIMD_128);

    // indexed access to enum value
    std::size_t index = 1;
    ASSERT_EQ(magic_enum::enum_value<Mode>(index), Mode::Auto);

    // edge cases
    ASSERT_FALSE(magic_enum::enum_cast<Mode>("table").has_value());
    ASSERT_FALSE(magic_enum::enum_cast<Mode>("auto").has_value());
    ASSERT_FALSE(magic_enum::enum_cast<Mode>(-1).has_value());
    ASSERT_FALSE(magic_enum::enum_cast<Mode>(99999).has_value());
}
} // namespace DB::tests
