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

#include <Operators/WindowTransformOp.h>

namespace DB
{
WindowTransformOp::WindowTransformOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id_,
    const WindowDescription & window_description_)
    : TransformOp(exec_context_, req_id_)
    , window_description(window_description_)
{}

void WindowTransformOp::transformHeaderImpl(Block & header_)
{
    assert(!action);
    action = std::make_unique<WindowTransformAction>(header_, window_description, log->identifier());
    header_ = action->output_header;
}

void WindowTransformOp::operateSuffixImpl()
{
    if likely (action)
        action->cleanUp();
}

ReturnOpStatus WindowTransformOp::transformImpl(Block & block)
{
    assert(action);
    assert(!action->input_is_finished);
    if unlikely (!block)
    {
        action->input_is_finished = true;
        action->tryCalculate();
        block = action->tryGetOutputBlock();
        return OperatorStatus::HAS_OUTPUT;
    }
    else
    {
        action->appendBlock(block);
        action->tryCalculate();
        block = action->tryGetOutputBlock();
        return block ? OperatorStatus::HAS_OUTPUT : OperatorStatus::NEED_INPUT;
    }
}

ReturnOpStatus WindowTransformOp::tryOutputImpl(Block & block)
{
    assert(action);
    block = action->tryGetOutputBlock();
    if unlikely (action->input_is_finished)
        return OperatorStatus::HAS_OUTPUT;
    else
        return block ? OperatorStatus::HAS_OUTPUT : OperatorStatus::NEED_INPUT;
}
} // namespace DB
