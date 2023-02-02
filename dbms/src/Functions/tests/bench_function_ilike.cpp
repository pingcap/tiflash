// Copyright 2023 PingCAP, Ltd.
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

namespace DB
{
namespace tests
{

class IlikeBench : public benchmark::Fixture
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColUInt8Type = typename TypeTraits<UInt8>::FieldType;

    ColumnsWithTypeAndName data1{
        toVec<String>("col0", std::vector<ColStringType>(1000000, "aaaaaaaaaaaaaaaaa")),
        toVec<String>("col1", std::vector<ColStringType>(1000000, "aaaaaaaaaaaaaaaaa"))};
    ColumnsWithTypeAndName data2{
        toVec<String>("col0", std::vector<ColStringType>(1000000, "AAAAAAAAAAAAAAAAA")),
        toVec<String>("col1", std::vector<ColStringType>(1000000, "AAAAAAAAAAAAAAAAA"))};
    ColumnsWithTypeAndName data3{
        toVec<String>("col0", std::vector<ColStringType>(1000000, "aAaAaAaAaAaAaAaAa")),
        toVec<String>("col1", std::vector<ColStringType>(1000000, "aAaAaAaAaAaAaAaAa"))};
    ColumnsWithTypeAndName data4{
        toVec<String>("col0", std::vector<ColStringType>(1000000, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯")),
        toVec<String>("col1", std::vector<ColStringType>(1000000, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯"))};
    ColumnsWithTypeAndName data5{
        toVec<String>("col0", std::vector<ColStringType>(1000000, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯")),
        toVec<String>("col1", std::vector<ColStringType>(1000000, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯"))};

    void SetUp(const benchmark::State &) override {}
};

BENCHMARK_DEFINE_F(IlikeBench, ilike)
(benchmark::State & state)
try
{
    FunctionIlike function_ilike;
    TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_UNICODE_CI);
    function_ilike.setCollator(collator);
    std::vector<Block> blocks{Block(data1), Block(data2), Block(data3), Block(data4), Block(data5)};
    ColumnNumbers arguments{0, 1};
    for (auto _ : state)
    {
        for (auto & block : blocks)
            function_ilike.executeImpl(block, arguments, 2);
    }
}
CATCH
BENCHMARK_REGISTER_F(IlikeBench, ilike)->Iterations(10);

BENCHMARK_DEFINE_F(IlikeBench, like)
(benchmark::State & state)
try
{
    FunctionLike function_like;
    TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_UNICODE_CI);
    function_like.setCollator(collator);
    std::vector<Block> blocks{Block(data1), Block(data2), Block(data3), Block(data4), Block(data5)};
    ColumnNumbers arguments{0, 1};
    for (auto _ : state)
    {
        for (auto & block : blocks)
            function_like.executeImpl(block, arguments, 2);
    }
}
CATCH
BENCHMARK_REGISTER_F(IlikeBench, ilike)->Iterations(10);

} // namespace tests
} // namespace DB
