// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Index/RSResult.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

TEST(RSResultTest, Not)
{
    ASSERT_EQ(!RSResult::Some, RSResult::Some);
    ASSERT_EQ(!RSResult::None, RSResult::All);
    ASSERT_EQ(!RSResult::All, RSResult::None);
    ASSERT_EQ(!RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(!RSResult::NoneNull, RSResult::AllNull);
    ASSERT_EQ(!RSResult::AllNull, RSResult::NoneNull);
}

TEST(RSResultTest, And)
{
    ASSERT_EQ(RSResult::Some && RSResult::Some, RSResult::Some);
    ASSERT_EQ(RSResult::Some && RSResult::None, RSResult::None);
    ASSERT_EQ(RSResult::Some && RSResult::All, RSResult::Some);
    ASSERT_EQ(RSResult::Some && RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::Some && RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::Some && RSResult::AllNull, RSResult::SomeNull);

    ASSERT_EQ(RSResult::None && RSResult::Some, RSResult::None);
    ASSERT_EQ(RSResult::None && RSResult::None, RSResult::None);
    ASSERT_EQ(RSResult::None && RSResult::All, RSResult::None);
    ASSERT_EQ(RSResult::None && RSResult::SomeNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::None && RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::None && RSResult::AllNull, RSResult::NoneNull);

    ASSERT_EQ(RSResult::All && RSResult::Some, RSResult::Some);
    ASSERT_EQ(RSResult::All && RSResult::None, RSResult::None);
    ASSERT_EQ(RSResult::All && RSResult::All, RSResult::All);
    ASSERT_EQ(RSResult::All && RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::All && RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::All && RSResult::AllNull, RSResult::AllNull);

    ASSERT_EQ(RSResult::SomeNull && RSResult::Some, RSResult::SomeNull);
    ASSERT_EQ(RSResult::SomeNull && RSResult::None, RSResult::NoneNull);
    ASSERT_EQ(RSResult::SomeNull && RSResult::All, RSResult::SomeNull);
    ASSERT_EQ(RSResult::SomeNull && RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::SomeNull && RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::SomeNull && RSResult::AllNull, RSResult::SomeNull);

    ASSERT_EQ(RSResult::NoneNull && RSResult::Some, RSResult::NoneNull);
    ASSERT_EQ(RSResult::NoneNull && RSResult::None, RSResult::NoneNull);
    ASSERT_EQ(RSResult::NoneNull && RSResult::All, RSResult::NoneNull);
    ASSERT_EQ(RSResult::NoneNull && RSResult::SomeNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::NoneNull && RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::NoneNull && RSResult::AllNull, RSResult::NoneNull);

    ASSERT_EQ(RSResult::AllNull && RSResult::Some, RSResult::SomeNull);
    ASSERT_EQ(RSResult::AllNull && RSResult::None, RSResult::NoneNull);
    ASSERT_EQ(RSResult::AllNull && RSResult::All, RSResult::AllNull);
    ASSERT_EQ(RSResult::AllNull && RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::AllNull && RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::AllNull && RSResult::AllNull, RSResult::AllNull);
}

TEST(RSResultTest, Or)
{
    ASSERT_EQ(RSResult::Some || RSResult::Some, RSResult::Some);
    ASSERT_EQ(RSResult::Some || RSResult::None, RSResult::Some);
    ASSERT_EQ(RSResult::Some || RSResult::All, RSResult::All);
    ASSERT_EQ(RSResult::Some || RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::Some || RSResult::NoneNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::Some || RSResult::AllNull, RSResult::AllNull);

    ASSERT_EQ(RSResult::None || RSResult::Some, RSResult::Some);
    ASSERT_EQ(RSResult::None || RSResult::None, RSResult::None);
    ASSERT_EQ(RSResult::None || RSResult::All, RSResult::All);
    ASSERT_EQ(RSResult::None || RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::None || RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::None || RSResult::AllNull, RSResult::AllNull);

    ASSERT_EQ(RSResult::All || RSResult::Some, RSResult::All);
    ASSERT_EQ(RSResult::All || RSResult::None, RSResult::All);
    ASSERT_EQ(RSResult::All || RSResult::All, RSResult::All);
    ASSERT_EQ(RSResult::All || RSResult::SomeNull, RSResult::All);
    ASSERT_EQ(RSResult::All || RSResult::NoneNull, RSResult::All);
    ASSERT_EQ(RSResult::All || RSResult::AllNull, RSResult::All);

    ASSERT_EQ(RSResult::SomeNull || RSResult::Some, RSResult::SomeNull);
    ASSERT_EQ(RSResult::SomeNull || RSResult::None, RSResult::SomeNull);
    ASSERT_EQ(RSResult::SomeNull || RSResult::All, RSResult::All);
    ASSERT_EQ(RSResult::SomeNull || RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::SomeNull || RSResult::NoneNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::SomeNull || RSResult::AllNull, RSResult::AllNull);

    ASSERT_EQ(RSResult::NoneNull || RSResult::Some, RSResult::SomeNull);
    ASSERT_EQ(RSResult::NoneNull || RSResult::None, RSResult::NoneNull);
    ASSERT_EQ(RSResult::NoneNull || RSResult::All, RSResult::All);
    ASSERT_EQ(RSResult::NoneNull || RSResult::SomeNull, RSResult::SomeNull);
    ASSERT_EQ(RSResult::NoneNull || RSResult::NoneNull, RSResult::NoneNull);
    ASSERT_EQ(RSResult::NoneNull || RSResult::AllNull, RSResult::AllNull);

    ASSERT_EQ(RSResult::AllNull || RSResult::Some, RSResult::AllNull);
    ASSERT_EQ(RSResult::AllNull || RSResult::None, RSResult::AllNull);
    ASSERT_EQ(RSResult::AllNull || RSResult::All, RSResult::All);
    ASSERT_EQ(RSResult::AllNull || RSResult::SomeNull, RSResult::AllNull);
    ASSERT_EQ(RSResult::AllNull || RSResult::NoneNull, RSResult::AllNull);
    ASSERT_EQ(RSResult::AllNull || RSResult::AllNull, RSResult::AllNull);
}
} // namespace DB::DM::tests
