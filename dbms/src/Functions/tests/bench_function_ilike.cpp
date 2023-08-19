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

/// this is a hack, include the cpp file so we can test MatchImpl directly
#include <Functions/FunctionsString.cpp>
#include <Functions/FunctionsStringSearch.cpp> // NOLINT

namespace DB
{
namespace tests
{

constexpr size_t data_num = 500000;

class IlikeBench : public benchmark::Fixture
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColUInt8Type = typename TypeTraits<UInt8>::FieldType;

    ColumnWithTypeAndName escape = createConstColumn<Int32>(1, static_cast<Int32>('\\'));

    ColumnsWithTypeAndName data1{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        escape};
    ColumnsWithTypeAndName data2{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "AAAAAAAAAAAAAAAAA")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "AAAAAAAAAAAAAAAAA")),
        escape};
    ColumnsWithTypeAndName data3{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "aAaAaAaAaAaAaAaAa")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "aAaAaAaAaAaAaAaAa")),
        escape};
    ColumnsWithTypeAndName data4{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯")),
        escape};
    ColumnsWithTypeAndName data5{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯")),
        escape};

    void SetUp(const benchmark::State &) override {}
};

class LikeBench : public benchmark::Fixture
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColUInt8Type = typename TypeTraits<UInt8>::FieldType;

    ColumnWithTypeAndName escape = createConstColumn<Int32>(1, static_cast<Int32>('\\'));

    ColumnsWithTypeAndName lower_data11{toVec<String>("col0", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa"))};
    ColumnsWithTypeAndName lower_data12{toVec<String>("col1", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa"))};

    ColumnsWithTypeAndName lower_data21{toVec<String>("col0", std::vector<ColStringType>(data_num, "AAAAAAAAAAAAAAAAA"))};
    ColumnsWithTypeAndName lower_data22{toVec<String>("col1", std::vector<ColStringType>(data_num, "AAAAAAAAAAAAAAAAA"))};

    ColumnsWithTypeAndName lower_data31{toVec<String>("col0", std::vector<ColStringType>(data_num, "aAaAaAaAaAaAaAaAa"))};
    ColumnsWithTypeAndName lower_data32{toVec<String>("col1", std::vector<ColStringType>(data_num, "aAaAaAaAaAaAaAaAa"))};

    ColumnsWithTypeAndName lower_data41{toVec<String>("col0", std::vector<ColStringType>(data_num, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯"))};
    ColumnsWithTypeAndName lower_data42{toVec<String>("col1", std::vector<ColStringType>(data_num, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯"))};

    ColumnsWithTypeAndName lower_data51{toVec<String>("col0", std::vector<ColStringType>(data_num, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯"))};
    ColumnsWithTypeAndName lower_data52{toVec<String>("col1", std::vector<ColStringType>(data_num, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯"))};

    ColumnsWithTypeAndName like_data1{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        escape};
    ColumnsWithTypeAndName like_data2{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        escape};
    ColumnsWithTypeAndName like_data3{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "aaaaaaaaaaaaaaaaa")),
        escape};
    ColumnsWithTypeAndName like_data4{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "嗯嗯嗯嗯嗯嗯嗯嗯嗯嗯")),
        escape};
    ColumnsWithTypeAndName like_data5{
        toVec<String>("col0", std::vector<ColStringType>(data_num, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯")),
        toVec<String>("col1", std::vector<ColStringType>(data_num, "a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯a嗯")),
        escape};

    void SetUp(const benchmark::State &) override {}
};

BENCHMARK_DEFINE_F(IlikeBench, ilike)
(benchmark::State & state)
try
{
    FunctionIlike3Args function_ilike;
    TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_BIN);
    function_ilike.setCollator(collator);
    std::vector<Block> blocks{Block(data1), Block(data2), Block(data3), Block(data4), Block(data5)};
    for (auto & block : blocks)
        block.insert({nullptr, std::make_shared<DataTypeNumber<UInt8>>(), "res"});
    ColumnNumbers arguments{0, 1, 2};
    for (auto _ : state)
    {
        for (auto & block : blocks)
            function_ilike.executeImpl(block, arguments, 3);
    }
}
CATCH
BENCHMARK_REGISTER_F(IlikeBench, ilike)->Iterations(10);

BENCHMARK_DEFINE_F(LikeBench, like)
(benchmark::State & state)
try
{
    FunctionLowerUTF8 function_lower;
    FunctionLike function_like;
    TiDB::TiDBCollatorPtr collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_BIN);
    function_like.setCollator(collator);
    std::vector<Block> lower_blocks{
        Block(lower_data11),
        Block(lower_data21),
        Block(lower_data31),
        Block(lower_data41),
        Block(lower_data51),
        Block(lower_data12),
        Block(lower_data22),
        Block(lower_data32),
        Block(lower_data42),
        Block(lower_data52)};
    std::vector<Block> like_blocks{Block(like_data1), Block(like_data2), Block(like_data3), Block(like_data4), Block(like_data5)};

    for (auto & block : lower_blocks)
        block.insert({nullptr, std::make_shared<DataTypeString>(), "res"});
    for (auto & block : like_blocks)
        block.insert({nullptr, std::make_shared<DataTypeNumber<UInt8>>(), "res"});

    ColumnNumbers lower_arguments{0, 1};
    ColumnNumbers like_arguments{0, 1, 2};
    for (auto _ : state)
    {
        for (auto & block : lower_blocks)
            function_lower.executeImpl(block, lower_arguments, 1);
        for (auto & block : like_blocks)
            function_like.executeImpl(block, like_arguments, 3);
    }
}
CATCH
BENCHMARK_REGISTER_F(LikeBench, like)->Iterations(10);

} // namespace tests
} // namespace DB
