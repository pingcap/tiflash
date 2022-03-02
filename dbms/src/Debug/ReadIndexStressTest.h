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
