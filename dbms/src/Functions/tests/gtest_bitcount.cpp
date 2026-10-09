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

#include <Common/Exception.h>
#include <DataTypes/DataTypeNothing.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

class TestFunctionBitCount : public DB::tests::FunctionTest
{
};

#define ASSERT_BITCOUNT(t1, result) \
    ASSERT_COLUMN_EQ(result, executeFunction("bitCount", {t1}))


TEST_F(TestFunctionBitCount, Simple)
try
{
    ASSERT_BITCOUNT(createColumn<Nullable<UInt64>>({1, 2, 4, 8, 16, 32, 100}), createColumn<Nullable<Int64>>({1, 1, 1, 1, 1, 1, 3}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int64>>({-1, -2, -4, -8, -16, -32, -100}), createColumn<Nullable<Int64>>({64, 63, 62, 61, 60, 59, 60}));
}
CATCH

TEST_F(TestFunctionBitCount, Boundary)
try
{
    ASSERT_BITCOUNT(createColumn<Nullable<Int64>>({0, INT64_MAX, INT64_MIN}), createColumn<Nullable<Int64>>({0, 63, 1}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt64>>({0, UINT64_MAX}), createColumn<Nullable<Int64>>({0, 64}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int32>>({0, INT32_MAX, INT32_MIN}), createColumn<Nullable<Int64>>({0, 31, 33}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt32>>({0, UINT32_MAX}), createColumn<Nullable<Int64>>({0, 32}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int16>>({0, INT16_MAX, INT16_MIN}), createColumn<Nullable<Int64>>({0, 15, 49}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt16>>({0, UINT16_MAX}), createColumn<Nullable<Int64>>({0, 16}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int8>>({0, INT8_MAX, INT8_MIN}), createColumn<Nullable<Int64>>({0, 7, 57}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt8>>({0, UINT8_MAX}), createColumn<Nullable<Int64>>({0, 8}));
}
CATCH

TEST_F(TestFunctionBitCount, NullTest)
try
{
    ASSERT_BITCOUNT(createColumn<Nullable<Int64>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int32>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int16>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int8>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt64>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt32>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt16>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt8>>({std::nullopt}), createColumn<Nullable<Int64>>({std::nullopt}));
}
CATCH

TEST_F(TestFunctionBitCount, TypeTest)
try
{
    ASSERT_BITCOUNT(createColumn<Nullable<Int8>>({1}), createColumn<Nullable<Int64>>({1}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int16>>({1}), createColumn<Nullable<Int64>>({1}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int32>>({1}), createColumn<Nullable<Int64>>({1}));
    ASSERT_BITCOUNT(createColumn<Nullable<Int64>>({1}), createColumn<Nullable<Int64>>({1}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<Int64>>({1}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt16>>({1}), createColumn<Nullable<Int64>>({1}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt32>>({1}), createColumn<Nullable<Int64>>({1}));
    ASSERT_BITCOUNT(createColumn<Nullable<UInt64>>({1}), createColumn<Nullable<Int64>>({1}));
}
CATCH

} // namespace tests
} // namespace DB