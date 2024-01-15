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

#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Debug/ReadIndexStressTest.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/ReadIndexWorker.h>
#include <Storages/KVStore/TMTContext.h>
#include <fmt/chrono.h>


namespace DB
{
const std::map<ReadIndexStressTest::TestType, std::string> ReadIndexStressTest::TestTypeName
    = {{ReadIndexStressTest::TestType::V1, "batch-read-index-v1"},
       {ReadIndexStressTest::TestType::Async, "async-read-index"}};

static const std::map<std::string, ReadIndexStressTest::TestType> TestName2Type
    = {{"batch-read-index-v1", ReadIndexStressTest::TestType::V1},
       {"async-read-index", ReadIndexStressTest::TestType::Async}};

ReadIndexStressTest::ReadIndexStressTest(const TMTContext & tmt_)
    : tmt(tmt_)
{
    MockStressTestCfg::enable = true;
    LOG_WARNING(logger, "enable MockStressTest");
}

ReadIndexStressTest::~ReadIndexStressTest()
{
    MockStressTestCfg::enable = false;
    LOG_WARNING(logger, "disable MockStressTest");
}

void ReadIndexStressTest::dbgFuncStressTest(Context & context, const ASTs & args, DBGInvoker::Printer printer)
{
    if (args.size() < 4)
        throw Exception(
            "Args not matched, should be: min_region_cnt, loop_cnt, type(batch-read-index-v1, async-read-index), "
            "concurrency",
            ErrorCodes::BAD_ARGUMENTS);
    auto min_region_cnt = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    auto loop_cnt = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    TestType ver = TestName2Type.at(typeid_cast<const ASTIdentifier &>(*args[2]).name);
    auto concurrency = safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    {
        if (min_region_cnt < 1 || loop_cnt < 1 || concurrency < 1)
            throw Exception(
                "Invalid args: `min_region_cnt < 1 or loop_cnt < 1 or concurrency < 1` ",
                ErrorCodes::LOGICAL_ERROR);
    }
    ReadIndexStressTest cxt{context.getTMTContext()};
    size_t req_cnt{};
    std::vector<TimeCost> tests_time_cost;
    {
        std::vector<std::deque<TimeCost>> tmp_tests_time_cost;
        cxt.runConcurrency(min_region_cnt, loop_cnt, ver, concurrency, req_cnt, tmp_tests_time_cost);

        for (const auto & a : tmp_tests_time_cost)
        {
            for (const auto & b : a)
            {
                tests_time_cost.emplace_back(b);
            }
        }
        std::sort(tests_time_cost.begin(), tests_time_cost.end());
    }
    size_t cnt = tests_time_cost.size();
    TimeCost min_tc = TimeCost ::max(), max_tc = TimeCost::min(), avg{}, median = tests_time_cost[cnt / 2];
    {
        for (const auto & b : tests_time_cost)
        {
            avg += b;
            min_tc = std::min(min_tc, b);
            max_tc = std::max(max_tc, b);
        }
        if (cnt % 2 == 0)
        {
            median = tests_time_cost[cnt / 2];
            median += tests_time_cost[(cnt / 2) - 1];
            median /= 2;
        }
        avg /= cnt;
    }
    std::string addition_info = max_tc >= std::chrono::seconds{10} ? "Error: meet timeout" : "";
    printer(fmt::format(
        "region count each round {}, loop count {}, concurrency {}, type `{}`, time cost min {}, max {}, avg {}, "
        "median {}, {}",
        req_cnt,
        loop_cnt,
        concurrency,
        TestTypeName.at(ver),
        min_tc,
        max_tc,
        avg,
        median,
        addition_info));
}

void ReadIndexStressTest::runConcurrency(
    size_t min_region_cnt,
    size_t loop_cnt,
    TestType ver,
    size_t concurrency,
    size_t & req_cnt,
    std::vector<std::deque<TimeCost>> & tests_time_cost)
{
    std::list<std::thread> threads;
    tests_time_cost.resize(concurrency);
    for (size_t cur_id = 0; cur_id < concurrency; ++cur_id)
    {
        threads.emplace_back(std::thread{[&, cur_id]() {
            std::string name = fmt::format("RdIdxStress-{}", cur_id);
            setThreadName(name.data());
            const auto & kvstore = *tmt.getKVStore();
            std::vector<kvrpcpb::ReadIndexRequest> reqs;
            reqs.reserve(min_region_cnt);
            if (kvstore.regionSize() == 0)
            {
                throw Exception("no exist region", ErrorCodes::LOGICAL_ERROR);
            }
            {
                size_t times = min_region_cnt / kvstore.regionSize();
                kvstore.traverseRegions([&](RegionID, const RegionPtr & region) {
                    for (size_t i = 0; i < times; ++i)
                        reqs.emplace_back(GenRegionReadIndexReq(*region, 1));
                });
                kvstore.traverseRegions([&](RegionID, const RegionPtr & region) {
                    if (reqs.size() < min_region_cnt)
                        reqs.emplace_back(GenRegionReadIndexReq(*region, 1));
                });
            }
            req_cnt = reqs.size();
            for (size_t j = 0; j < loop_cnt; ++j)
            {
                auto ts = tmt.getPDClient()->getTS();
                for (auto & r : reqs)
                    r.set_start_ts(ts);
                auto time_cost = run(reqs, ver);
                tests_time_cost[cur_id].emplace_back(time_cost);
            }
        }});
    }
    for (auto & t : threads)
        t.join();
}

ReadIndexStressTest::TimeCost ReadIndexStressTest::run(std::vector<kvrpcpb::ReadIndexRequest> reqs, TestType ver)
{
    const auto & kvstore = *tmt.getKVStore();
    size_t timeout_ms = 10 * 1000;
    const auto wrap_time_cost = [&](std::function<void()> && f) {
        auto start_time = std::chrono::steady_clock::now();
        f();
        auto end_time = std::chrono::steady_clock ::now();
        auto time_cost = std::chrono::duration_cast<TimeCost>(end_time - start_time);
        LOG_INFO(logger, "time cost {}", time_cost);
        return time_cost;
    };
    switch (ver)
    {
    case TestType::V1:
        return wrap_time_cost([&]() {
            LOG_INFO(logger, "begin to run `{}`: req size {}, ", TestTypeName.at(ver), reqs.size());
            kvstore.getProxyHelper()->batchReadIndex_v1(reqs, timeout_ms);
        });
    case TestType::Async:
        return wrap_time_cost([&]() {
            if (!kvstore.read_index_worker_manager)
            {
                LOG_ERROR(logger, "no read_index_worker_manager");
                return;
            }
            LOG_INFO(logger, "begin to run `{}`: req size {}", TestTypeName.at(ver), reqs.size());
            for (size_t i = 0; i < reqs.size(); ++i)
            {
                auto & req = reqs[i];
                req.mutable_context()->set_region_id(
                    req.context().region_id() + MockStressTestCfg::RegionIdPrefix * (i + 1));
            }
            LOG_INFO(logger, "add prefix {} to each region id", MockStressTestCfg::RegionIdPrefix);
            auto resps = kvstore.batchReadIndex(reqs, timeout_ms);
            for (const auto & resp : resps)
            {
                if (resp.first.read_index() == 0)
                    throw Exception("meet region error", ErrorCodes::LOGICAL_ERROR);
            }
        });
    default:
        throw Exception("unknown type", ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace DB
