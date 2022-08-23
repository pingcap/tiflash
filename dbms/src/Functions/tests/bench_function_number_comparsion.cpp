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
#include <TestUtils/FunctionTestUtils.h>
#include <benchmark/benchmark.h>

#include <boost/random.hpp>

namespace DB::tests
{

class NumberComparsionBench : public benchmark::Fixture
{
public:
    using ColInt64Type = typename TypeTraits<Int64>::FieldType;
    ColumnsWithTypeAndName data;
    const int row_num = 100000;

    void SetUp(const benchmark::State &) override
    {
        DataTypePtr data_type = std::make_shared<DataTypeInt64>();
        auto tmp_col_int64_1 = data_type->createColumn();
        auto tmp_col_int64_2 = data_type->createColumn();

        std::uniform_int_distribution<int64_t> dist64(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
        boost::random::mt19937 mt;
        for (int i = 0; i < row_num; ++i)
        {
            tmp_col_int64_1->insert(Field(static_cast<Int64>(static_cast<Int64>(dist64(mt)))));
            tmp_col_int64_2->insert(Field(static_cast<Int64>(static_cast<Int64>(dist64(mt)))));
        }

        data = {
            createConstColumn<Int64>(row_num, 0, "col0"),
            ColumnWithTypeAndName(std::move(tmp_col_int64_1), data_type, "col1"),
            ColumnWithTypeAndName(std::move(tmp_col_int64_2), data_type, "col2"),
            toVec<Int64>("result", std::vector<ColInt64Type>{})};
    }
};

#define BENCH_EQ_NUMBER(CLASS_NAME, col_1, col_2)         \
    BENCHMARK_DEFINE_F(NumberComparsionBench, CLASS_NAME) \
    (benchmark::State & state)                            \
    try                                                   \
    {                                                     \
        FunctionEquals fe;                                \
        Block block(data);                                \
        ColumnNumbers arguments{col_1, col_2};            \
        for (auto _ : state)                              \
        {                                                 \
            fe.executeImpl(block, arguments, 3);          \
        }                                                 \
    }                                                     \
    CATCH                                                 \
    BENCHMARK_REGISTER_F(NumberComparsionBench, CLASS_NAME)->Iterations(100);


#define BENCH_LESS_NUMBER(CLASS_NAME, col_1, col_2)       \
    BENCHMARK_DEFINE_F(NumberComparsionBench, CLASS_NAME) \
    (benchmark::State & state)                            \
    try                                                   \
    {                                                     \
        FunctionLess fl;                                  \
        Block block(data);                                \
        ColumnNumbers arguments{col_1, col_2};            \
        for (auto _ : state)                              \
        {                                                 \
            fl.executeImpl(block, arguments, 3);          \
        }                                                 \
    }                                                     \
    CATCH                                                 \
    BENCHMARK_REGISTER_F(NumberComparsionBench, CLASS_NAME)->Iterations(100);


BENCH_EQ_NUMBER(constantEqVector, 0, 1);
BENCH_EQ_NUMBER(vectorEqVector, 1, 2);

BENCH_LESS_NUMBER(constantLessVector, 0, 1);
BENCH_LESS_NUMBER(vectorLessVector, 1, 2);

} // namespace DB::tests