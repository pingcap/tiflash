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

#include <Operators/Expand2TransformOp.h>

namespace DB
{
OperatorStatus Expand2TransformOp::transformImpl(Block & block)
{
    if (likely(block))
        expand_transform_action.transform(block);
    // empty block should also be output.
    return OperatorStatus::HAS_OUTPUT;
}

OperatorStatus Expand2TransformOp::tryOutputImpl(Block & block)
{
    if (expand_transform_action.tryOutput(block))
        return OperatorStatus::HAS_OUTPUT;
    // current cached block is exhausted, need a new one.
    return OperatorStatus::NEED_INPUT;
}

void Expand2TransformOp::transformHeaderImpl(Block & header_)
{
    header_ = expand_transform_action.getHeader();
}
} // namespace DB
