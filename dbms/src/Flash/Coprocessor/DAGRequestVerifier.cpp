#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGRequestVerifier.h>
#include <Flash/Statistics/traverseExecutors.h>

#include <functional>
#include <unordered_set>

namespace DB
{
namespace
{
#define CHECK(condition, msg)                                           \
    if (!(condition))                                                   \
    {                                                                   \
        throw TiFlashException((msg), Errors::Coprocessor::BadRequest); \
    }


using Rule = std::function<void(const tipb::DAGRequest *, const DAGContext &)>;

Rule check_mode = [](const tipb::DAGRequest * dag_request, const DAGContext & dag_context) {
    CHECK(dag_request->has_root_executor() != (dag_request->executors_size() > 0), "One and only one of root_executor and executors exists");
    if (dag_context.isMPPTask())
    {
        CHECK(dag_request->has_root_executor(), "for mpp mode, dag_request.has_root_executor() must be true");
        CHECK(dag_request->root_executor().has_exchange_sender(), "for mpp mode, dag_request.root_executor() must be ExchangeSender");
    }
};

Rule check_executor_id = [](const tipb::DAGRequest * dag_request, const DAGContext & dag_context) {
    std::unordered_set<String> set;
    traverseExecutors(dag_request, [&](const tipb::Executor & executor) {
        CHECK(dag_context.isMPPTask() && executor.has_executor_id(), "for mpp mode, execute.has_executor_id() must be true");
        if (executor.has_executor_id())
        {
            CHECK(!executor.executor_id().empty(), "executor_id is blank");
            CHECK(set.find(executor.executor_id()) != set.end(), fmt::format("executor_id({}) duplicate", executor.executor_id()));
            set.insert(executor.executor_id());
        }
        return true;
    });
};

Rule misc = [](const tipb::DAGRequest * dag_request, const DAGContext &) {
    CHECK(!dag_request->output_offsets().empty(), "dag_request.output_offsets() is empty");
};

#undef CHECK
} // namespace

void DAGRequestVerifier::verify(const tipb::DAGRequest * request)
{
    static std::vector<Rule> rules{check_mode, check_executor_id, misc};

    for (const auto & rule : rules)
        rule(request, dag_context);
}
} // namespace DB