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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Exception.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Storages/Columnar/ColumnarSourceOp.h>

#include <magic_enum.hpp>

namespace DB
{

OperatorStatus RNColumnarSourceOp::readImpl(Block & block)
{
    auto queue_result = shared_queue->tryPop(block);
    switch (queue_result)
    {
    case MPMCQueueResult::EMPTY:
        setNotifyFuture(shared_queue.get());
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case MPMCQueueResult::OK:
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::FINISHED:
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::CANCELLED:
        return OperatorStatus::CANCELLED;
    default:
        RUNTIME_CHECK_MSG(
            false,
            "Unexpected queue result for RNColumnarSourceOp: {}",
            magic_enum::enum_name(queue_result));
    }
}

} // namespace DB
#endif
