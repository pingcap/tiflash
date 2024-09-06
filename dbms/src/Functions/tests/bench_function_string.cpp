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

#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsStringSearch.h>
#include <TestUtils/FunctionTestUtils.h>
#include <benchmark/benchmark.h>

<<<<<<<< HEAD:dbms/src/Functions/tests/bench_collation.cpp
/// this is a hack, include the cpp file so we can test MatchImpl directly
========
/// this is a hack, include the cpp file so we can test functions directly
#include <Functions/FunctionsString.cpp> // NOLINT
>>>>>>>> b30c1f5090 (Improve the performance of `length` and `ascii` functions (#9345)):dbms/src/Functions/tests/bench_function_string.cpp
#include <Functions/FunctionsStringSearch.cpp> // NOLINT

namespace DB
{
namespace tests
{

class CollationBench : public benchmark::Fixture
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColUInt8Type = typename TypeTraits<UInt8>::FieldType;

    ColumnsWithTypeAndName data{toVec<String>("col0", std::vector<ColStringType>(1000000, "aaaaaaaaaaaaa")),
                                toVec<String>("col1", std::vector<ColStringType>(1000000, "aaaaaaaaaaaaa")),
                                toVec<UInt8>("result", std::vector<ColUInt8Type>{})};

    ColumnsWithTypeAndName like_data{toVec<String>("col0", std::vector<ColStringType>(1000000, "qwdgefwabchfue")),
                                     createConstColumn<String>(1000000, "%abc%"),
                                     createConstColumn<Int32>(1000000, static_cast<Int32>('\\')),
                                     toVec<UInt8>("result", std::vector<ColUInt8Type>{})};
};

class CollationLessBench : public CollationBench
{
public:
    void SetUp(const benchmark::State &) override {}
};

class CollationEqBench : public CollationBench
{
public:
    void SetUp(const benchmark::State &) override {}
};

class CollationLikeBench : public CollationBench
{
public:
    void SetUp(const benchmark::State &) override {}
};

#define BENCH_LESS_COLLATOR(collator)                                                                     \
    BENCHMARK_DEFINE_F(CollationLessBench, collator)                                                      \
    (benchmark::State & state)                                                                            \
    try                                                                                                   \
    {                                                                                                     \
        FunctionLess fl;                                                                                  \
        TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::collator); \
        fl.setCollator(collator);                                                                         \
        Block block(data);                                                                                \
        ColumnNumbers arguments{0, 1};                                                                    \
        for (auto _ : state)                                                                              \
        {                                                                                                 \
            fl.executeImpl(block, arguments, 2);                                                          \
        }                                                                                                 \
    }                                                                                                     \
    CATCH                                                                                                 \
    BENCHMARK_REGISTER_F(CollationLessBench, collator)->Iterations(10);


#define BENCH_EQ_COLLATOR(collator)                                                                       \
    BENCHMARK_DEFINE_F(CollationEqBench, collator)                                                        \
    (benchmark::State & state)                                                                            \
    try                                                                                                   \
    {                                                                                                     \
        FunctionEquals fe;                                                                                \
        TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::collator); \
        fe.setCollator(collator);                                                                         \
        Block block(data);                                                                                \
        ColumnNumbers arguments{0, 1};                                                                    \
        for (auto _ : state)                                                                              \
        {                                                                                                 \
            fe.executeImpl(block, arguments, 2);                                                          \
        }                                                                                                 \
    }                                                                                                     \
    CATCH                                                                                                 \
    BENCHMARK_REGISTER_F(CollationEqBench, collator)->Iterations(10);


#define BENCH_LIKE_COLLATOR(collator)                                                                     \
    BENCHMARK_DEFINE_F(CollationLikeBench, collator)                                                      \
    (benchmark::State & state)                                                                            \
    try                                                                                                   \
    {                                                                                                     \
        FunctionLike3Args fl;                                                                             \
        TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::collator); \
        fl.setCollator(collator);                                                                         \
        Block block(like_data);                                                                           \
        ColumnNumbers arguments{0, 1, 2};                                                                 \
        for (auto _ : state)                                                                              \
        {                                                                                                 \
            fl.executeImpl(block, arguments, 3);                                                          \
        }                                                                                                 \
    }                                                                                                     \
    CATCH                                                                                                 \
    BENCHMARK_REGISTER_F(CollationLikeBench, collator)->Iterations(10);


BENCH_LESS_COLLATOR(UTF8MB4_BIN);
BENCH_LESS_COLLATOR(UTF8MB4_GENERAL_CI);
BENCH_LESS_COLLATOR(UTF8MB4_UNICODE_CI);
BENCH_LESS_COLLATOR(UTF8_BIN);
BENCH_LESS_COLLATOR(UTF8_GENERAL_CI);
BENCH_LESS_COLLATOR(UTF8_UNICODE_CI);
BENCH_LESS_COLLATOR(ASCII_BIN);
BENCH_LESS_COLLATOR(BINARY);
BENCH_LESS_COLLATOR(LATIN1_BIN);

BENCH_EQ_COLLATOR(UTF8MB4_BIN);
BENCH_EQ_COLLATOR(UTF8MB4_GENERAL_CI);
BENCH_EQ_COLLATOR(UTF8MB4_UNICODE_CI);
BENCH_EQ_COLLATOR(UTF8_BIN);
BENCH_EQ_COLLATOR(UTF8_GENERAL_CI);
BENCH_EQ_COLLATOR(UTF8_UNICODE_CI);
BENCH_EQ_COLLATOR(ASCII_BIN);
BENCH_EQ_COLLATOR(BINARY);
BENCH_EQ_COLLATOR(LATIN1_BIN);

BENCH_LIKE_COLLATOR(UTF8MB4_BIN);
BENCH_LIKE_COLLATOR(UTF8MB4_GENERAL_CI);
BENCH_LIKE_COLLATOR(UTF8MB4_UNICODE_CI);
BENCH_LIKE_COLLATOR(UTF8_BIN);
BENCH_LIKE_COLLATOR(UTF8_GENERAL_CI);
BENCH_LIKE_COLLATOR(UTF8_UNICODE_CI);
BENCH_LIKE_COLLATOR(ASCII_BIN);
BENCH_LIKE_COLLATOR(BINARY);
BENCH_LIKE_COLLATOR(LATIN1_BIN);

class LengthBench : public benchmark::Fixture
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;

    ColumnsWithTypeAndName data1{toVec<String>("col", std::vector<ColStringType>(data_num, ""))};
    ColumnsWithTypeAndName data2{toVec<String>("col", std::vector<ColStringType>(data_num, "aaaaaaaaaa"))};
    ColumnsWithTypeAndName data3{toVec<String>("col", std::vector<ColStringType>(data_num, "啊aaaaaaaa"))};

    void SetUp(const benchmark::State &) override {}
};

BENCHMARK_DEFINE_F(LengthBench, bench)
(benchmark::State & state)
try
{
    FunctionLength function_length;
    std::vector<Block> blocks{Block(data1), Block(data2), Block(data3)};
    for (auto & block : blocks)
        block.insert({nullptr, std::make_shared<DataTypeNumber<UInt8>>(), "res"});
    ColumnNumbers arguments{0};
    for (auto _ : state)
    {
        for (auto & block : blocks)
            function_length.executeImpl(block, arguments, 1);
    }
}
CATCH
BENCHMARK_REGISTER_F(LengthBench, bench)->Iterations(10);

class ASCIIBench : public benchmark::Fixture
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;

    ColumnsWithTypeAndName data1{toVec<String>("col", std::vector<ColStringType>(data_num, ""))};
    ColumnsWithTypeAndName data2{toVec<String>("col", std::vector<ColStringType>(data_num, "aaaaaaaaaa"))};
    ColumnsWithTypeAndName data3{toVec<String>("col", std::vector<ColStringType>(data_num, "啊aaaaaaaa"))};

    void SetUp(const benchmark::State &) override {}
};

BENCHMARK_DEFINE_F(ASCIIBench, bench)
(benchmark::State & state)
try
{
    FunctionASCII function_ascii;
    std::vector<Block> blocks{Block(data1), Block(data2), Block(data3)};
    for (auto & block : blocks)
        block.insert({nullptr, std::make_shared<DataTypeNumber<UInt8>>(), "res"});
    ColumnNumbers arguments{0};
    for (auto _ : state)
    {
        for (auto & block : blocks)
            function_ascii.executeImpl(block, arguments, 1);
    }
}
CATCH
BENCHMARK_REGISTER_F(ASCIIBench, bench)->Iterations(10);

} // namespace tests
} // namespace DB
