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

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsCommon.h>
#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
// bench class 1 -- test the performance comparsion between filter once and filter twice 
// create one block, and generate two filter 
Block filterBlock(Block &block, IColumn::Filter& filter) {
    const size_t passed_count = countBytesInFilter(filter);
    Block res;
    for (size_t index = 0; index < block.columns(); ++index)
    {
        auto & column = block.getByPosition(index);
        column.column = column.column->filter(filter, passed_count);
        res.insert(std::move(column));
    }
    return res;
}

Block createBlock(size_t rows, size_t columns) {
    Block block;
    for (size_t i = 0; i < columns; ++i) {
        auto name = "column" + std::to_string(i);
        block.insert(DB::tests::createColumn<UInt64>(
                std::vector<UInt64>(rows, 200),
                name,
                i));
    }
    return block;
}

std::set<int> createRandomNum(size_t max_value, size_t num){
    std::set<int> res;

    std::random_device rd;
    std::default_random_engine eng(rd());
    std::uniform_int_distribution<int> distr(0, max_value - 1);

    while (res.size() <  num) {
        int value = distr(eng);
        if (res.find(value) == res.end()) {
            res.insert(value);
        }
    }

    return res;
}

IColumn::Filter createFilter(size_t rows, size_t filter_rows) {
    IColumn::Filter filter{};
    filter.resize(rows);

    auto filter_num_set = createRandomNum(rows, filter_rows);

    for (size_t i = 0; i < rows; ++i)
    {
        if (filter_num_set.find(i) == filter_num_set.end()){
            filter[i] = 1;
        } else {
            filter[i] = 0;
        }
    }

    return filter;
}

constexpr size_t num_iterations_test = 100000;

static void benchFilterTwice(benchmark::State & state){
    auto block_row = state.range(0);
    auto colummn_num = state.range(1);
    auto filter_count_first = state.range(2);
    auto filter_count_second = state.range(3);

    Block block = createBlock(block_row, colummn_num);
    auto filter_first = createFilter(block_row, filter_count_first);
    auto filter_second = createFilter(block_row - filter_count_first, filter_count_second);

    std::vector<Block> block_vec(num_iterations_test, block);
    int index = 0;
    for (auto _ : state)
    {
        auto block_after_filter_first = filterBlock(block_vec[index], filter_first);
        auto block_after_filter_second = filterBlock(block_after_filter_first, filter_second);
        index++;
    }
}

static void benchFilterOnce(benchmark::State & state){
    auto block_row = state.range(0);
    auto colummn_num = state.range(1);
    auto filter_count_first = state.range(2);
    auto filter_count_second = state.range(3);
    
    Block block = createBlock(block_row, colummn_num);
    auto filter = createFilter(block_row, filter_count_first + filter_count_second);

    std::vector<Block> block_vec(num_iterations_test, block);
    int index = 0;
    for (auto _ : state)
    {
        auto block_after_filter = filterBlock(block_vec[index], filter);
        index++;
    }
}


BENCHMARK(benchFilterTwice)->Iterations(num_iterations_test)->Args({1000,5,10,10})->Args({1000,5,100,100})->Args({1000,10,10,10})->Args({1000,30,10,10})->Args({500,10,10,10})->Args({5000,10,10,10});
BENCHMARK(benchFilterOnce)->Iterations(num_iterations_test)->Args({1000,5,10,10})->Args({1000,5,100,100})->Args({1000,10,10,10})->Args({1000,30,10,10})->Args({500,10,10,10})->Args({5000,10,10,10});

}