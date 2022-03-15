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

#pragma once

#include <Debug/DBGInvoker.h>
#include <common/logger_useful.h>

namespace kvrpcpb
{
class ReadIndexRequest;
}

namespace DB
{
class TMTContext;
class Context;

class ReadIndexStressTest
{
public:
    enum class TestType
    {
        V1,
        Async,
    };
    using TimeCost = std::chrono::milliseconds;

    static const std::map<TestType, std::string> TestTypeName;

    // min_region_cnt, loop_cnt, type(batch-read-index-v1, async-read-index), concurrency
    static void dbgFuncStressTest(Context & context, const ASTs & args, DBGInvoker::Printer);

    TimeCost run(
        std::vector<kvrpcpb::ReadIndexRequest> reqs,
        TestType ver);

    void runConcurrency(size_t min_region_cnt,
                        size_t loop_cnt,
                        TestType ver,
                        size_t concurrency,
                        size_t & req_cnt,
                        std::vector<std::deque<TimeCost>> & tests_time_cost);

    explicit ReadIndexStressTest(const TMTContext &);
    ~ReadIndexStressTest();

private:
    const TMTContext & tmt;
    Poco::Logger * logger{&Poco::Logger::get("ReadIndexStressTest")};
};


} // namespace DB
