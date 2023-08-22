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
    case tipb::ExecType::TypeWindow:
        return Children{&executor.window().child()};
    case tipb::ExecType::TypeSort:
        return Children{&executor.sort().child()};
    case tipb::ExecType::TypeTopN:
        return Children{&executor.topn().child()};
    case tipb::ExecType::TypeLimit:
        return Children{&executor.limit().child()};
    case tipb::ExecType::TypeExpand:
        return Children{&executor.expand().child()};
    case tipb::ExecType::TypeExpand2:
        return Children{&executor.expand2().child()};
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