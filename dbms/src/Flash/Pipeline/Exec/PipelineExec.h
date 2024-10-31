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

#pragma once

#include <Operators/Operator.h>

#include <memory>

namespace DB
{
// The executor for push model operator.
// A pipeline will generate multiple pipeline_execs.
// data flow: source --> transform --> .. --> transform --> sink
class PipelineExec : private boost::noncopyable
{
public:
    PipelineExec(
        SourceOpPtr && source_op_,
        TransformOps && transform_ops_,
        SinkOpPtr && sink_op_,
        bool has_pipeline_breaker_wait_time_,
        uint64_t minTSO_wait_time_in_ns_);

    void executePrefix();
    void executeSuffix();

    OperatorStatus execute();

    OperatorStatus executeIO();

    OperatorStatus await();

    void notify();

    void finalizeProfileInfo(UInt64 queuing_time, UInt64 pipeline_breaker_wait_time);

private:
    inline OperatorStatus executeImpl();

    inline OperatorStatus executeIOImpl();

    inline OperatorStatus awaitImpl();

    inline OperatorStatus fetchBlock(Block & block, size_t & start_transform_op_index);

    ALWAYS_INLINE void fillAwaitable(Operator * op)
    {
        assert(!awaitable);
        assert(op);
        awaitable = op;
    }

    ALWAYS_INLINE void fillIOOp(Operator * op)
    {
        assert(!io_op);
        assert(op);
        io_op = op;
    }

    ALWAYS_INLINE void fillWaitingForNotifyOp(Operator * op)
    {
        assert(!waiting_for_notify);
        assert(op);
        waiting_for_notify = op;
    }

private:
    SourceOpPtr source_op;
    TransformOps transform_ops;
    SinkOpPtr sink_op;
    bool has_pipeline_breaker_wait_time;
    uint64_t minTSO_wait_time_in_ns;

    // hold the operator which is ready for executing await.
    Operator * awaitable = nullptr;

    // hold the operator which is ready for executing io.
    Operator * io_op = nullptr;

    // hold the operator which is waiting for notify.
    Operator * waiting_for_notify = nullptr;
};
using PipelineExecPtr = std::unique_ptr<PipelineExec>;
// a set of pipeline_execs running in parallel.
using PipelineExecGroup = std::vector<PipelineExecPtr>;
} // namespace DB
