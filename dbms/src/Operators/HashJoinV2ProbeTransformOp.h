// Copyright 2024 PingCAP, Inc.
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

#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/JoinV2/HashJoinProbe.h>
#include <Operators/Operator.h>

namespace DB
{
class HashJoinV2ProbeTransformOp : public TransformOp
{
public:
    HashJoinV2ProbeTransformOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const HashJoinPtr & join_,
        size_t op_index_);

    String getName() const override { return "HashJoinV2ProbeTransformOp"; }

protected:
    OperatorStatus transformImpl(Block & block) override;

    OperatorStatus tryOutputImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

    void operateSuffixImpl() override;

private:
    OperatorStatus onOutput(Block & block);

private:
    HashJoinPtr join_ptr;
    size_t op_index;

    JoinProbeContext probe_context;

    size_t joined_rows = 0;
    size_t scan_hash_map_rows = 0;
};
} // namespace DB
