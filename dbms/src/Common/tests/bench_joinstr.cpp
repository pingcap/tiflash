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
#include <Common/FmtUtils.h>


namespace DB
{
namespace bench
{

class JoinStr : public benchmark::Fixture
{
protected:
    static constexpr size_t num_repeat = 10;

public:
    std::vector<String> vs;
    void SetUp(const ::benchmark::State & /*state*/)
    {
        init();
    }

    void init() {
        for (size_t i=0; i < 10000; ++i)
        {
            if (i % 2)
            vs.push_back("hhhh");
            else
             vs.push_back("rrrrr");
        }
    }

};

BENCHMARK_DEFINE_F(JoinStr, v1)
(benchmark::State & state)
{
   
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            FmtBuffer fb;
            fb.joinStr(vs.begin(), vs.end(),  [&fb](const auto & s) { fb.append(s);}, ", ");
            fb.toString();
        }
    }
}
BENCHMARK_REGISTER_F(JoinStr, v1)->Iterations(200);

BENCHMARK_DEFINE_F(JoinStr, v2)
(benchmark::State & state)
{
   
    for (auto _ : state)
    {
        for (size_t i = 0; i < num_repeat; ++i)
        {
            FmtBuffer fb;
            fb.joinStrv2(vs.begin(), vs.end(),  [](const auto & s) -> String { return s; },", ");
            fb.toString();
        }
    }
}
BENCHMARK_REGISTER_F(JoinStr, v2)->Iterations(200);



} // namespace bench

}