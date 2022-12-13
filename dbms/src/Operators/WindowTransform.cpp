// Copyright 2022 PingCAP, Ltd.
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

#include <Operators/WindowTransform.h>

namespace DB
{
OperatorStatus WindowTransform::transform(Block & block)
{
    if (unlikely(action.input_is_finished))
    {
        block = {};
        return OperatorStatus::PASS;
    }
    if (unlikely(!block))
    {
        action.input_is_finished = true;
        action.tryCalculate();
        block = action.tryGetOutputBlock();
        return OperatorStatus::PASS;
    }
    else
    {
        action.appendBlock(block);
        action.tryCalculate();
        block = action.tryGetOutputBlock();
        return block ? OperatorStatus::MORE_OUTPUT : OperatorStatus::NEED_MORE;
    }
}

Block WindowTransform::fetchBlock()
{
    return action.tryGetOutputBlock();
}
} // namespace DB
