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

namespace DB
{
namespace tests
{

class CollationBench : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) override
    {
        data.push_back(toVec<String>("col1", std::vector<ColStringType>(1000000, "aaaaaaaaaaaaa")));
        data.push_back(toVec<String>("col2", std::vector<ColStringType>(1000000, "aaaaaaaaaaaaa")));
        data.push_back(toVec<UInt8>("result", std::vector<ColUInt8Type>{}));
    }

public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColUInt8Type = typename TypeTraits<UInt8>::FieldType;

    ColumnsWithTypeAndName data{};
};

#define BENCH_COLLATOR(collator)                                                                          \
    BENCHMARK_DEFINE_F(CollationBench, collator)                                                          \
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
    BENCHMARK_REGISTER_F(CollationBench, collator)->Iterations(10);


BENCH_COLLATOR(UTF8MB4_BIN);
BENCH_COLLATOR(UTF8MB4_GENERAL_CI);
BENCH_COLLATOR(UTF8MB4_UNICODE_CI);

BENCH_COLLATOR(UTF8_BIN);
BENCH_COLLATOR(UTF8_GENERAL_CI);
BENCH_COLLATOR(UTF8_UNICODE_CI);

BENCH_COLLATOR(ASCII_BIN);

BENCH_COLLATOR(BINARY);
BENCH_COLLATOR(LATIN1_BIN);

} // namespace tests
} // namespace DB
