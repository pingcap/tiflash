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

#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB::tests
{
template <typename T>
using Limits = std::numeric_limits<T>;

// TODO support unsigned int as the test framework not supports unsigned int so far.
class LeadLag : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override { ExecutorTest::initializeContext(); }

    template <typename IntType>
    void testInt()
    {
        executeFunctionAndAssert(
            toNullableVec<IntType>({Limits<IntType>::max(), Limits<IntType>::min(), 4, {}, 6, 0, 8, {}}),
            Lead2(value_col, lit(Field(static_cast<UInt64>(1)))),
            {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
             toNullableVec<IntType>(/*value*/ {1, Limits<IntType>::max(), Limits<IntType>::min(), 4, 5, 6, 0, 8})});
        executeFunctionAndAssert(
            toNullableVec<IntType>({{}, 1, Limits<IntType>::max(), Limits<IntType>::min(), {}, 5, 6, 0}),
            Lag2(value_col, lit(Field(static_cast<UInt64>(1)))),
            {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
             toNullableVec<IntType>(/*value*/ {1, Limits<IntType>::max(), Limits<IntType>::min(), 4, 5, 6, 0, 8})});
    }

    template <typename FloatType>
    void testFloat()
    {
        executeFunctionAndAssert(
            toNullableVec<FloatType>({Limits<FloatType>::max(), Limits<FloatType>::min(), 4.4, {}, 6.6, 0, 8.8, {}}),
            Lead2(value_col, lit(Field(static_cast<UInt64>(1)))),
            {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
             toNullableVec<FloatType>(
                 /*value*/ {1, Limits<FloatType>::max(), Limits<FloatType>::min(), 4.4, 5.5, 6.6, 0, 8.8})});
        executeFunctionAndAssert(
            toNullableVec<FloatType>({{}, 1, Limits<FloatType>::max(), Limits<FloatType>::min(), {}, 5.5, 6.6, 0}),
            Lag2(value_col, lit(Field(static_cast<UInt64>(1)))),
            {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
             toNullableVec<FloatType>(
                 /*value*/ {1, Limits<FloatType>::max(), Limits<FloatType>::min(), 4.4, 5.5, 6.6, 0, 8.8})});
    }
};

TEST_F(LeadLag, oneArg)
try
{
    executeFunctionAndAssert(
        toNullableVec<String>({"2", "3", "4", {}, "6", "7", "8", {}}),
        Lead1(value_col),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, "1", "2", "3", {}, "5", "6", "7"}),
        Lag1(value_col),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
}
CATCH

TEST_F(LeadLag, twoArgs)
try
{
    // arg2 == 0
    executeFunctionAndAssert(
        toNullableVec<String>({"1", "2", "3", "4", "5", "6", "7", "8"}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(0)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"1", "2", "3", "4", "5", "6", "7", "8"}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(0)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    // arg2 < partition_size
    executeFunctionAndAssert(
        toNullableVec<String>({"2", "3", "4", {}, "6", "7", "8", {}}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(1)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"3", "4", {}, {}, "7", "8", {}, {}}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(2)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"4", {}, {}, {}, "8", {}, {}, {}}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(3)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, "1", "2", "3", {}, "5", "6", "7"}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(1)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, "1", "2", {}, {}, "5", "6"}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(2)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, "1", {}, {}, {}, "5"}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(3)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    // arg2 >= partition_size
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(4)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(5)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(4)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(5)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
}
CATCH

TEST_F(LeadLag, threeArgs)
try
{
    // arg2 == 0
    executeFunctionAndAssert(
        toNullableVec<String>({"1", "2", "3", "4", "5", "6", "7", "8"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(0))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"1", "2", "3", "4", "5", "6", "7", "8"}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(0))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    // arg2 < partition_size
    executeFunctionAndAssert(
        toNullableVec<String>({"2", "3", "4", "0", "6", "7", "8", "0"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"0", "1", "2", "3", "0", "5", "6", "7"}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    // arg2 >= partition_size
    executeFunctionAndAssert(
        toNullableVec<String>({"0", "0", "0", "0", "0", "0", "0", "0"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(4))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"0", "0", "0", "0", "0", "0", "0", "0"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(5))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"0", "0", "0", "0", "0", "0", "0", "0"}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(4))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"0", "0", "0", "0", "0", "0", "0", "0"}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(5))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    // test arg column type
    executeFunctionAndAssert(
        toNullableVec<String>({"2", "3", "4", "4", "6", "7", "8", "8"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(1))), value_col),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"0", "0", "0", {}, "0", "0", "0", "8"}),
        Lead3(lit(Field(String("0"))), lit(Field(static_cast<UInt64>(1))), value_col),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", {}, "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toVec<String>({"0", "0", "1", "1", "0", "0", "1", "1"}),
        Lead3(lit(Field(String("0"))), lit(Field(static_cast<UInt64>(2))), lit(Field(String("1")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
}
CATCH

TEST_F(LeadLag, testNull)
try
{
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, "aaaaaa", {}, {}, {}, "aaaaaa"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("aaaaaa")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {{}, {}, {}, {}, {}, {}, {}, {}})});
    executeFunctionAndAssert(
        toNullableVec<String>({"aaaaaa", {}, {}, {}, "aaaaaa", {}, {}, {}}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("aaaaaa")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {{}, {}, {}, {}, {}, {}, {}, {}})});

    executeFunctionAndAssert(
        toNullableVec<String>({"2", "3", "4", {}, "6", "7", "8", {}}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(1)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, "1", "2", "3", {}, "5", "6", "7"}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(1)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}}),
        Lead2(value_col, lit(Field(static_cast<UInt64>(1)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {{}, {}, {}, {}, {}, {}, {}, {}})});
    executeFunctionAndAssert(
        toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}}),
        Lag2(value_col, lit(Field(static_cast<UInt64>(1)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {{}, {}, {}, {}, {}, {}, {}, {}})});

    executeFunctionAndAssert(
        toNullableVec<String>({"aaaaaa", "aaaaaa", {}, {}, "aaaaaa", "aaaaaa", {}, {}}),
        Lead2(lit(Field(String("aaaaaa"))), lit(Field(static_cast<UInt64>(2)))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {{}, {}, {}, {}, {}, {}, {}, {}})});
    executeFunctionAndAssert(
        toNullableVec<String>({"aaaaaa", "aaaaaa", "aaaaaa", {}, "aaaaaa", "aaaaaa", "aaaaaa", {}}),
        Lead1(lit(Field(String("aaaaaa")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {{}, {}, {}, {}, {}, {}, {}, {}})});
}
CATCH

TEST_F(LeadLag, String)
try
{
    // normal case
    executeFunctionAndAssert(
        toNullableVec<String>({"2", "3", "4", "0", "6", "7", "8", "0"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"0", "1", "2", "3", "0", "5", "6", "7"}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("0")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    // blank string
    executeFunctionAndAssert(
        toNullableVec<String>({"", "", "", "aaaaaa", "", "", "", "aaaaaa"}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("aaaaaa")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"", "", "", "", "", "", "", ""})});
    executeFunctionAndAssert(
        toNullableVec<String>({"aaaaaa", "", "", "", "aaaaaa", "", "", ""}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("aaaaaa")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"", "", "", "", "", "", "", ""})});

    executeFunctionAndAssert(
        toNullableVec<String>({"2", "3", "4", "", "6", "7", "8", ""}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});
    executeFunctionAndAssert(
        toNullableVec<String>({"", "1", "2", "3", "", "5", "6", "7"}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8"})});

    executeFunctionAndAssert(
        toNullableVec<String>({"", "", "", "", "", "", "", ""}),
        Lead3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"", "", "", "", "", "", "", ""})});
    executeFunctionAndAssert(
        toNullableVec<String>({"", "", "", "", "", "", "", ""}),
        Lag3(value_col, lit(Field(static_cast<UInt64>(1))), lit(Field(String("")))),
        {toNullableVec<Int64>(/*partition*/ {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>(/*order*/ {1, 2, 3, 4, 5, 6, 7, 8}),
         toNullableVec<String>(/*value*/ {"", "", "", "", "", "", "", ""})});
}
CATCH

TEST_F(LeadLag, Int)
try
{
    testInt<Int8>();
    testInt<Int16>();
    testInt<Int32>();
    testInt<Int64>();
}
CATCH

TEST_F(LeadLag, Float)
try
{
    testFloat<Float32>();
    testFloat<Float64>();
}
CATCH

// TODO support decimal

} // namespace DB::tests
