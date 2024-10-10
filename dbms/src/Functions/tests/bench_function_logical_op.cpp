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

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionsBinaryLogical.h>
#include <Functions/FunctionsLogical.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <benchmark/benchmark.h>

#include <memory>

namespace DB
{
namespace tests
{

constexpr size_t rows = 10000;

class LogicalOpBench : public benchmark::Fixture
{
protected:
    ColumnWithTypeAndName col_not_null_uint64_1;
    ColumnWithTypeAndName col_not_null_uint64_2;
    ColumnWithTypeAndName col_nullable_uint64_1;
    ColumnWithTypeAndName col_nullable_uint64_2;
    ColumnWithTypeAndName col_not_null_uint8_1;
    ColumnWithTypeAndName col_not_null_uint8_2;
    ColumnWithTypeAndName col_not_null_uint8_3;
    ColumnWithTypeAndName col_not_null_uint8_4;
    ColumnWithTypeAndName col_not_null_uint8_5;
    ColumnWithTypeAndName col_not_null_uint8_6;
    ColumnWithTypeAndName col_nullable_uint8_1;
    ColumnWithTypeAndName col_nullable_uint8_2;
    ColumnWithTypeAndName col_nullable_uint8_3;
    ColumnWithTypeAndName col_nullable_uint8_4;
    ColumnWithTypeAndName col_nullable_uint8_5;
    ColumnWithTypeAndName col_nullable_uint8_6;
    ColumnWithTypeAndName col_constant_null;
    ColumnWithTypeAndName col_constant_true;
    ColumnWithTypeAndName col_constant_false;
    DataTypePtr not_null_result_type;
    DataTypePtr nullable_result_type;
    ColumnsWithTypeAndName not_null_uint8_columns;
    ColumnsWithTypeAndName nullable_uint8_columns;

public:
    void SetUp(const benchmark::State &) override
    {
        ColumnGeneratorOpts opts{rows, "UInt64", DataDistribution::RANDOM};
        col_not_null_uint64_1 = ColumnGenerator::instance().generate(opts);
        col_not_null_uint64_2 = ColumnGenerator::instance().generate(opts);
        opts.type_name = "Nullable(UInt64)";
        col_nullable_uint64_1 = ColumnGenerator::instance().generate(opts);
        col_nullable_uint64_2 = ColumnGenerator::instance().generate(opts);
        opts.type_name = "UInt8";
        col_not_null_uint8_1 = ColumnGenerator::instance().generate(opts);
        not_null_uint8_columns.push_back(col_not_null_uint8_1);
        col_not_null_uint8_2 = ColumnGenerator::instance().generate(opts);
        not_null_uint8_columns.push_back(col_not_null_uint8_2);
        col_not_null_uint8_3 = ColumnGenerator::instance().generate(opts);
        not_null_uint8_columns.push_back(col_not_null_uint8_3);
        col_not_null_uint8_4 = ColumnGenerator::instance().generate(opts);
        not_null_uint8_columns.push_back(col_not_null_uint8_4);
        col_not_null_uint8_5 = ColumnGenerator::instance().generate(opts);
        not_null_uint8_columns.push_back(col_not_null_uint8_5);
        col_not_null_uint8_6 = ColumnGenerator::instance().generate(opts);
        not_null_uint8_columns.push_back(col_not_null_uint8_6);
        opts.type_name = "Nullable(UInt8)";
        col_nullable_uint8_1 = ColumnGenerator::instance().generate(opts);
        nullable_uint8_columns.push_back(col_nullable_uint8_1);
        col_nullable_uint8_2 = ColumnGenerator::instance().generate(opts);
        nullable_uint8_columns.push_back(col_nullable_uint8_2);
        col_nullable_uint8_3 = ColumnGenerator::instance().generate(opts);
        nullable_uint8_columns.push_back(col_nullable_uint8_3);
        col_nullable_uint8_4 = ColumnGenerator::instance().generate(opts);
        nullable_uint8_columns.push_back(col_nullable_uint8_4);
        col_nullable_uint8_5 = ColumnGenerator::instance().generate(opts);
        nullable_uint8_columns.push_back(col_nullable_uint8_5);
        col_nullable_uint8_6 = ColumnGenerator::instance().generate(opts);
        nullable_uint8_columns.push_back(col_nullable_uint8_6);
        col_constant_null = col_nullable_uint64_1;
        col_constant_true = col_nullable_uint64_1;
        col_constant_false = col_nullable_uint64_1;
        col_constant_null.column = col_constant_null.type->createColumnConst(rows, Null());
        col_constant_true.column
            = makeNullable(DataTypeUInt8().createColumnConst(rows, toField(static_cast<UInt64>(10))));
        col_constant_false.column
            = makeNullable(DataTypeUInt8().createColumnConst(rows, toField(static_cast<UInt64>(0))));
        not_null_result_type = std::make_shared<DataTypeNumber<UInt8>>();
        nullable_result_type = makeNullable(not_null_result_type);
    }
};

#define BINARY_LOGICAL_BENCH(COL1_NAME, COL2_NAME, OP_NAME)                           \
    BENCHMARK_DEFINE_F(LogicalOpBench, binaryLogical_##OP_NAME##COL1_NAME##COL2_NAME) \
    (benchmark::State & state)                                                        \
    try                                                                               \
    {                                                                                 \
        FunctionBinary##OP_NAME function_binary;                                      \
        ColumnsWithTypeAndName columns;                                               \
        auto col_1 = col##COL1_NAME;                                                  \
        auto col_2 = col##COL2_NAME;                                                  \
        columns.push_back(col_1);                                                     \
        columns.push_back(col_2);                                                     \
        Block input(columns);                                                         \
        if (col_1.type->isNullable() || col_2.type->isNullable())                     \
            input.insert({nullptr, nullable_result_type, "res"});                     \
        else                                                                          \
            input.insert({nullptr, not_null_result_type, "res"});                     \
        ColumnNumbers arguments{0, 1};                                                \
        for (auto _ : state)                                                          \
        {                                                                             \
            function_binary.executeImpl(input, arguments, 2);                         \
        }                                                                             \
    }                                                                                 \
    CATCH                                                                             \
    BENCHMARK_REGISTER_F(LogicalOpBench, binaryLogical_##OP_NAME##COL1_NAME##COL2_NAME)->Iterations(1000);

#define ANY_LOGICAL_BENCH(COL1_NAME, COL2_NAME, OP_NAME)                           \
    BENCHMARK_DEFINE_F(LogicalOpBench, AnyLogical_##OP_NAME##COL1_NAME##COL2_NAME) \
    (benchmark::State & state)                                                     \
    try                                                                            \
    {                                                                              \
        Function##OP_NAME function_any;                                            \
        ColumnsWithTypeAndName columns;                                            \
        auto col_1 = col##COL1_NAME;                                               \
        auto col_2 = col##COL2_NAME;                                               \
        columns.push_back(col_1);                                                  \
        columns.push_back(col_2);                                                  \
        Block input(columns);                                                      \
        if (col_1.type->isNullable() || col_2.type->isNullable())                  \
            input.insert({nullptr, nullable_result_type, "res"});                  \
        else                                                                       \
            input.insert({nullptr, not_null_result_type, "res"});                  \
        ColumnNumbers arguments{0, 1};                                             \
        for (auto _ : state)                                                       \
        {                                                                          \
            function_any.executeImpl(input, arguments, 2);                         \
        }                                                                          \
    }                                                                              \
    CATCH                                                                          \
    BENCHMARK_REGISTER_F(LogicalOpBench, AnyLogical_##OP_NAME##COL1_NAME##COL2_NAME)->Iterations(1000);

#define LOGICAL_BENCH(COL1_NAME, COL2_NAME, OP_NAME)    \
    BINARY_LOGICAL_BENCH(COL1_NAME, COL2_NAME, OP_NAME) \
    ANY_LOGICAL_BENCH(COL1_NAME, COL2_NAME, OP_NAME)

// warm up
LOGICAL_BENCH(_nullable_uint8_2, _nullable_uint8_1, And);
LOGICAL_BENCH(_not_null_uint8_2, _nullable_uint8_1, And);
LOGICAL_BENCH(_not_null_uint8_2, _not_null_uint8_1, And);
// and
//LOGICAL_BENCH(_not_null_uint64_1, _not_null_uint64_2, And);
//LOGICAL_BENCH(_nullable_uint64_1, _nullable_uint64_2, And);
//LOGICAL_BENCH(_not_null_uint64_1, _nullable_uint64_2, And);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_true, And);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_false, And);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_null, And);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_true, And);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_false, And);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_null, And);
//LOGICAL_BENCH(_nullable_uint8_1, _constant_true, And);
//LOGICAL_BENCH(_nullable_uint8_1, _constant_false, And);
//LOGICAL_BENCH(_nullable_uint8_1, _constant_null, And);
//LOGICAL_BENCH(_nullable_uint64_1, _constant_true, And);
//LOGICAL_BENCH(_nullable_uint64_1, _constant_false, And);
//LOGICAL_BENCH(_nullable_uint64_1, _constant_null, And);

// or
//LOGICAL_BENCH(_not_null_uint64_1, _not_null_uint64_2, Or);
//LOGICAL_BENCH(_nullable_uint64_1, _nullable_uint64_2, Or);
//LOGICAL_BENCH(_not_null_uint64_1, _nullable_uint64_2, Or);
LOGICAL_BENCH(_nullable_uint8_1, _nullable_uint8_2, Or);
LOGICAL_BENCH(_not_null_uint8_1, _nullable_uint8_2, Or);
LOGICAL_BENCH(_not_null_uint8_1, _not_null_uint8_2, Or);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_true, Or);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_false, Or);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_null, Or);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_true, Or);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_false, Or);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_null, Or);
//LOGICAL_BENCH(_nullable_uint8_1, _constant_true, Or);
//LOGICAL_BENCH(_nullable_uint8_1, _constant_false, Or);
//LOGICAL_BENCH(_nullable_uint8_1, _constant_null, Or);
//LOGICAL_BENCH(_nullable_uint64_1, _constant_true, Or);
//LOGICAL_BENCH(_nullable_uint64_1, _constant_false, Or);
//LOGICAL_BENCH(_nullable_uint64_1, _constant_null, Or);
LOGICAL_BENCH(_nullable_uint8_1, _nullable_uint8_2, And);
LOGICAL_BENCH(_not_null_uint8_1, _nullable_uint8_2, And);
LOGICAL_BENCH(_not_null_uint8_1, _not_null_uint8_2, And);

// xor
//LOGICAL_BENCH(_not_null_uint64_1, _not_null_uint64_2, Xor);
LOGICAL_BENCH(_not_null_uint8_1, _not_null_uint8_2, Xor);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_true, Xor);
//LOGICAL_BENCH(_not_null_uint64_1, _constant_false, Xor);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_true, Xor);
//LOGICAL_BENCH(_not_null_uint8_1, _constant_false, Xor);
#define BINARY_LOGICAL_BENCH_MULTI_PARAM(PARAM_NUM, OP_NAME, NULLABLE)                         \
    BENCHMARK_DEFINE_F(LogicalOpBench, binaryLogicalMultiParam_##OP_NAME##PARAM_NUM##NULLABLE) \
    (benchmark::State & state)                                                                 \
    try                                                                                        \
    {                                                                                          \
        FunctionBinary##OP_NAME function_binary;                                               \
        ColumnsWithTypeAndName columns;                                                        \
        if ((PARAM_NUM) > 6)                                                                   \
            throw Exception("not supported");                                                  \
        ColumnNumbers arguments;                                                               \
        if ((NULLABLE) == 1)                                                                   \
        {                                                                                      \
            for (size_t i = 0; i < (PARAM_NUM); ++i)                                           \
            {                                                                                  \
                columns.push_back(nullable_uint8_columns[i]);                                  \
                arguments.push_back(i);                                                        \
            }                                                                                  \
        }                                                                                      \
        else                                                                                   \
        {                                                                                      \
            for (size_t i = 0; i < (PARAM_NUM); ++i)                                           \
            {                                                                                  \
                columns.push_back(not_null_uint8_columns[i]);                                  \
                arguments.push_back(i);                                                        \
            }                                                                                  \
        }                                                                                      \
        Block input(columns);                                                                  \
        if (columns[0].column->isColumnNullable())                                             \
            input.insert({nullptr, nullable_result_type, "res"});                              \
        else                                                                                   \
            input.insert({nullptr, not_null_result_type, "res"});                              \
        for (auto _ : state)                                                                   \
        {                                                                                      \
            function_binary.executeImpl(input, arguments, (PARAM_NUM));                        \
        }                                                                                      \
    }                                                                                          \
    CATCH                                                                                      \
    BENCHMARK_REGISTER_F(LogicalOpBench, binaryLogicalMultiParam_##OP_NAME##PARAM_NUM##NULLABLE)->Iterations(1000);

#define ANY_LOGICAL_BENCH_MULTI_PARAM(PARAM_NUM, OP_NAME, NULLABLE)                         \
    BENCHMARK_DEFINE_F(LogicalOpBench, anyLogicalMultiParam_##OP_NAME##PARAM_NUM##NULLABLE) \
    (benchmark::State & state)                                                              \
    try                                                                                     \
    {                                                                                       \
        Function##OP_NAME function_binary;                                                  \
        ColumnsWithTypeAndName columns;                                                     \
        if ((PARAM_NUM) > 6)                                                                \
            throw Exception("not supported");                                               \
        ColumnNumbers arguments;                                                            \
        if ((NULLABLE) == 1)                                                                \
        {                                                                                   \
            for (size_t i = 0; i < (PARAM_NUM); ++i)                                        \
            {                                                                               \
                columns.push_back(nullable_uint8_columns[i]);                               \
                arguments.push_back(i);                                                     \
            }                                                                               \
        }                                                                                   \
        else                                                                                \
        {                                                                                   \
            for (size_t i = 0; i < (PARAM_NUM); ++i)                                        \
            {                                                                               \
                columns.push_back(not_null_uint8_columns[i]);                               \
                arguments.push_back(i);                                                     \
            }                                                                               \
        }                                                                                   \
        Block input(columns);                                                               \
        if (columns[0].column->isColumnNullable())                                          \
            input.insert({nullptr, nullable_result_type, "res"});                           \
        else                                                                                \
            input.insert({nullptr, not_null_result_type, "res"});                           \
        for (auto _ : state)                                                                \
        {                                                                                   \
            function_binary.executeImpl(input, arguments, (PARAM_NUM));                     \
        }                                                                                   \
    }                                                                                       \
    CATCH                                                                                   \
    BENCHMARK_REGISTER_F(LogicalOpBench, anyLogicalMultiParam_##OP_NAME##PARAM_NUM##NULLABLE)->Iterations(1000);

BINARY_LOGICAL_BENCH_MULTI_PARAM(3, And, 0);
ANY_LOGICAL_BENCH_MULTI_PARAM(3, And, 0);
BINARY_LOGICAL_BENCH_MULTI_PARAM(4, And, 0);
ANY_LOGICAL_BENCH_MULTI_PARAM(4, And, 0);
BINARY_LOGICAL_BENCH_MULTI_PARAM(5, And, 0);
ANY_LOGICAL_BENCH_MULTI_PARAM(5, And, 0);
BINARY_LOGICAL_BENCH_MULTI_PARAM(6, And, 0);
ANY_LOGICAL_BENCH_MULTI_PARAM(6, And, 0);
BINARY_LOGICAL_BENCH_MULTI_PARAM(3, And, 1);
ANY_LOGICAL_BENCH_MULTI_PARAM(3, And, 1);
BINARY_LOGICAL_BENCH_MULTI_PARAM(4, And, 1);
ANY_LOGICAL_BENCH_MULTI_PARAM(4, And, 1);
BINARY_LOGICAL_BENCH_MULTI_PARAM(5, And, 1);
ANY_LOGICAL_BENCH_MULTI_PARAM(5, And, 1);
BINARY_LOGICAL_BENCH_MULTI_PARAM(6, And, 1);
ANY_LOGICAL_BENCH_MULTI_PARAM(6, And, 1);

} // namespace tests
} // namespace DB
