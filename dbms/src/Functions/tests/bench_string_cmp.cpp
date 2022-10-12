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

#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsStringSearch.h>
#include <TestUtils/FunctionTestUtils.h>
#include <benchmark/benchmark.h>

/// this is a hack, include the cpp file so we can test MatchImpl directly
#include <Functions/FunctionsStringSearch.cpp> // NOLINT

namespace DB::tests
{

class StringCmpBench : public benchmark::Fixture
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColUInt8Type = typename TypeTraits<UInt8>::FieldType;

    ColumnsWithTypeAndName data{toVec<String>("col0", std::vector<ColStringType>(10000000, "aaaaaaaaaaaaa")),
                                toVec<String>("col1", std::vector<ColStringType>(10000000, "aaaaaaaaaaaaa")),
                                toVec<UInt8>("result", std::vector<ColUInt8Type>{})};

    ColumnsWithTypeAndName like_data{toVec<String>("col0", std::vector<ColStringType>(10000000, "qwdgefwabchfue")),
                                     createConstColumn<String>(10000000, "%abc%"),
                                     createConstColumn<Int32>(10000000, static_cast<Int32>('\\')),
                                     toVec<UInt8>("result", std::vector<ColUInt8Type>{})};
};

class StringLessBench : public StringCmpBench
{
public:
    void SetUp(const benchmark::State &) override {}
};

class StringEqBench : public StringCmpBench
{
public:
    void SetUp(const benchmark::State &) override {}
};

class StringLikeBench : public StringCmpBench
{
public:
    void SetUp(const benchmark::State &) override {}
};

#define BENCH_LESS_COLLATOR(collator)             \
    BENCHMARK_DEFINE_F(StringLessBench, collator) \
    (benchmark::State & state)                    \
    try                                           \
    {                                             \
        FunctionLess fl;                          \
        Block block(data);                        \
        ColumnNumbers arguments{0, 1};            \
        for (auto _ : state)                      \
        {                                         \
            fl.executeImpl(block, arguments, 2);  \
        }                                         \
    }                                             \
    CATCH                                         \
    BENCHMARK_REGISTER_F(StringLessBench, collator)->Iterations(10);


#define BENCH_EQ_COLLATOR(collator)              \
    BENCHMARK_DEFINE_F(StringEqBench, collator)  \
    (benchmark::State & state)                   \
    try                                          \
    {                                            \
        FunctionEquals fe;                       \
        Block block(data);                       \
        ColumnNumbers arguments{0, 1};           \
        for (auto _ : state)                     \
        {                                        \
            fe.executeImpl(block, arguments, 2); \
        }                                        \
    }                                            \
    CATCH                                        \
    BENCHMARK_REGISTER_F(StringEqBench, collator)->Iterations(10);


#define BENCH_LIKE_COLLATOR(collator)             \
    BENCHMARK_DEFINE_F(StringLikeBench, collator) \
    (benchmark::State & state)                    \
    try                                           \
    {                                             \
        FunctionLike3Args fl;                     \
        Block block(like_data);                   \
        ColumnNumbers arguments{0, 1, 2};         \
        for (auto _ : state)                      \
        {                                         \
            fl.executeImpl(block, arguments, 3);  \
        }                                         \
    }                                             \
    CATCH                                         \
    BENCHMARK_REGISTER_F(StringLikeBench, collator)->Iterations(10);


BENCH_LESS_COLLATOR(basic);

BENCH_EQ_COLLATOR(basic);

BENCH_LIKE_COLLATOR(basic);

} // namespace DB::tests
