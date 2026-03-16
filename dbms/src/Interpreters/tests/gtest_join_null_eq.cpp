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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Join.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
constexpr auto * test_key_name = "k";

Block makeSampleBlock(const DataTypePtr & key_type)
{
    return Block{{key_type->createColumn(), key_type, test_key_name}};
}

JoinPtr makeTestJoin(const DataTypePtr & key_type, const std::vector<UInt8> & is_null_eq)
{
    SpillConfig build_spill_config("/tmp", "join_null_eq_build", 0, 0, 0, nullptr);
    SpillConfig probe_spill_config("/tmp", "join_null_eq_probe", 0, 0, 0, nullptr);
    return std::make_shared<Join>(
        Names{test_key_name},
        Names{test_key_name},
        is_null_eq,
        ASTTableJoin::Kind::Inner,
        "join_null_eq_test",
        0,
        0,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{1, 0, 0},
        NamesAndTypes{{test_key_name, key_type}},
        RegisterOperatorSpillContext{},
        nullptr,
        TiDB::TiDBCollators{},
        JoinNonEqualConditions{},
        1024,
        0,
        "",
        "",
        0,
        true);
}
} // namespace

TEST(JoinNullEqTest, NullableNullEqKeyForcesSerializedJoinMapMethod)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto join = makeTestJoin(nullable_int_type, {1});
    join->initBuild(makeSampleBlock(nullable_int_type), 1);

    ASSERT_EQ(join->getJoinMapMethod(), JoinMapMethod::serialized);
}

TEST(JoinNullEqTest, DefaultMethodSelectionRemainsForOtherCases)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto nullable_non_null_eq_join = makeTestJoin(nullable_int_type, {0});
    nullable_non_null_eq_join->initBuild(makeSampleBlock(nullable_int_type), 1);
    ASSERT_EQ(nullable_non_null_eq_join->getJoinMapMethod(), JoinMapMethod::key32);

    auto int_type = std::make_shared<DataTypeInt32>();
    auto non_nullable_null_eq_join = makeTestJoin(int_type, {1});
    non_nullable_null_eq_join->initBuild(makeSampleBlock(int_type), 1);
    ASSERT_EQ(non_nullable_null_eq_join->getJoinMapMethod(), JoinMapMethod::key32);
}

} // namespace DB::tests
