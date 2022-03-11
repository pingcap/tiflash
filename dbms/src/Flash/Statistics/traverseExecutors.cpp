#include <Common/TiFlashException.h>
#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
Children getChildren(const tipb::Executor & executor)
{
    switch (executor.tp())
    {
    case tipb::ExecType::TypeTableScan:
    case tipb::ExecType::TypePartitionTableScan:
        return {};
    case tipb::ExecType::TypeJoin:
        return {&executor.join().children(0), &executor.join().children(1)};
    case tipb::ExecType::TypeIndexScan:
        throw TiFlashException("IndexScan is not supported", Errors::Coprocessor::Unimplemented);
    case tipb::ExecType::TypeSelection:
        return Children{&executor.selection().child()};
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        return Children{&executor.aggregation().child()};
    case tipb::ExecType::TypeTopN:
        return Children{&executor.topn().child()};
    case tipb::ExecType::TypeLimit:
        return Children{&executor.limit().child()};
    case tipb::ExecType::TypeProjection:
        return Children{&executor.projection().child()};
    case tipb::ExecType::TypeExchangeSender:
        return Children{&executor.exchange_sender().child()};
    case tipb::ExecType::TypeExchangeReceiver:
        return {};
    case tipb::ExecType::TypeKill:
        throw TiFlashException("Kill executor is not supported", Errors::Coprocessor::Unimplemented);
    default:
        throw TiFlashException("Should not reach here", Errors::Coprocessor::Internal);
    }
}
} // namespace DB