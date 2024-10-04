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
#include <Functions/FunctionsBinaryLogical.h>
#include <Functions/FunctionsLogical.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <benchmark/benchmark.h>

#include <memory>

#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"

namespace DB
{
namespace tests
{

constexpr size_t data_num = 10000;

class LogicalOpBench : public benchmark::Fixture
{
protected:
    ColumnWithTypeAndName not_null_uint64_1;
    ColumnWithTypeAndName not_null_uint64_2;
    ColumnWithTypeAndName nullable_uint64_1;
    ColumnWithTypeAndName nullable_uint64_2;
    ColumnWithTypeAndName not_null_uint8_1;
    ColumnWithTypeAndName not_null_uint8_2;
    ColumnWithTypeAndName nullable_uint8_1;
    ColumnWithTypeAndName nullable_uint8_2;

public:
    void SetUp(const benchmark::State &) override
    {
        ColumnGeneratorOpts opts{data_num, "UInt64", DataDistribution::RANDOM};
        not_null_uint64_1 = ColumnGenerator::instance().generate(opts);
        not_null_uint64_2 = ColumnGenerator::instance().generate(opts);
        opts.type_name = "Nullable(UInt64)";
        nullable_uint64_1 = ColumnGenerator::instance().generate(opts);
        nullable_uint64_2 = ColumnGenerator::instance().generate(opts);
        opts.type_name = "UInt8";
        not_null_uint8_1 = ColumnGenerator::instance().generate(opts);
        not_null_uint8_2 = ColumnGenerator::instance().generate(opts);
        opts.type_name = "Nullable(UInt8)";
        nullable_uint8_1 = ColumnGenerator::instance().generate(opts);
        nullable_uint8_2 = ColumnGenerator::instance().generate(opts);
    }
};

BENCHMARK_DEFINE_F(LogicalOpBench, binaryLogical)
(benchmark::State & state)
try
{
    FunctionBinaryAnd function_binary_and;
    ColumnsWithTypeAndName columns;
    columns.push_back(nullable_uint64_1);
    columns.push_back(nullable_uint64_2);
    Block input(columns);
    auto uint8_type = std::make_shared<DataTypeNumber<UInt8>>();
    auto nullable_uint8_type = makeNullable(uint8_type);
    input.insert({nullptr, nullable_uint8_type, "res"});
    ColumnNumbers arguments{0, 1};
    for (auto _ : state)
    {
        function_binary_and.executeImpl(input, arguments, 2);
    }
}
CATCH
BENCHMARK_REGISTER_F(LogicalOpBench, binaryLogical)->Iterations(1000);

BENCHMARK_DEFINE_F(LogicalOpBench, AnyLogical)
(benchmark::State & state)
try
{
    FunctionAnd function_and;
    ColumnsWithTypeAndName columns;
    columns.push_back(nullable_uint64_1);
    columns.push_back(nullable_uint64_2);
    Block input(columns);
    auto uint8_type = std::make_shared<DataTypeNumber<UInt8>>();
    auto nullable_uint8_type = makeNullable(uint8_type);
    input.insert({nullptr, nullable_uint8_type, "res"});
    ColumnNumbers arguments{0, 1};
    for (auto _ : state)
    {
        function_and.executeImpl(input, arguments, 2);
    }
}
CATCH
BENCHMARK_REGISTER_F(LogicalOpBench, AnyLogical)->Iterations(1000);

} // namespace tests
} // namespace DB
