#include <Common/TiFlashException.h>
#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
Children getChildren(const tipb::Executor & executor)
{
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
        return {};
    case tipb::ExecType::TypeJoin:
        return {&executor.join().children(0), &executor.join().children(1)};
    case tipb::ExecType::TypeIndexScan:
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        return &executor.selection().child();
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        return &executor.aggregation().child();
    case tipb::ExecType::TypeTopN:
        return &executor.topn().child();
    case tipb::ExecType::TypeLimit:
        return &executor.limit().child();
    case tipb::ExecType::TypeProjection:
        return &executor.projection().child();
    case tipb::ExecType::TypeExchangeSender:
        return &executor.exchange_sender().child();
    case tipb::ExecType::TypeExchangeReceiver:
        return {};
    case tipb::ExecType::TypeKill:
        throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
    default:
        throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
    }
}
} // namespace DB