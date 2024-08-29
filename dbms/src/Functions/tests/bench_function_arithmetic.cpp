// Copyright 2024 PingCAP, Inc.
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


#include <Functions/plus.h>
#include <TestUtils/FunctionTestUtils.h>
#include <benchmark/benchmark.h>
#include <random>
#include "common/types.h"

namespace DB
{
namespace tests
{
class PlusBench : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) override {}
};

BENCHMARK_DEFINE_F(PlusBench, plus)
(benchmark::State & state)
try
{
    PlusImpl<Int64, Int64> p1;
    PlusImpl<UInt64, UInt64> p2;
    PlusImpl<Int64, UInt64> p3;
    PlusImpl<UInt64, Int64> p4;

    std::default_random_engine dre;
    std::uniform_int_distribution<Int64> signed_rand(-10000, 10000);
    std::uniform_int_distribution<UInt64> unsigned_rand(0, 10000);

    signed_rand.param(std::uniform_int_distribution<Int64>::param_type{-10000, 10000});
    unsigned_rand.param(std::uniform_int_distribution<UInt64>::param_type{0, 10000});

    Int64 val_num = 3000000;
    std::vector<Int64> signed_val1;
    std::vector<Int64> signed_val2;
    std::vector<UInt64> unsigned_val1;
    std::vector<UInt64> unsigned_val2;

    signed_val1.reserve(val_num);
    signed_val2.reserve(val_num);
    unsigned_val1.reserve(val_num);
    unsigned_val2.reserve(val_num);
    for (Int64 i = 0; i < val_num; i++)
    {
        signed_val1.push_back(signed_rand(dre));
        signed_val2.push_back(signed_rand(dre));
        unsigned_val1.push_back(unsigned_rand(dre));
        unsigned_val2.push_back(unsigned_rand(dre));
    }

    Int64 tmp = 0;

    for (auto _ : state)
    {
        for (Int64 i = 0; i < val_num; i++)
        {
            tmp += p1.apply(signed_val1[i], signed_val2[i]);
            tmp += p2.apply(unsigned_val1[i], unsigned_val2[i]);
            tmp += p3.apply(signed_val1[i], unsigned_val2[i]);
            tmp += p4.apply(unsigned_val1[i], signed_val2[i]);
        }
    }

    std::cout << tmp;
}
CATCH
BENCHMARK_REGISTER_F(PlusBench, plus)->Iterations(10);
} // namespace tests
} // namespace DB