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

#include <Common/Exception.h>
#include <Operators/AutoPassThroughAggregateTransform.h>

#include <magic_enum.hpp>

namespace DB
{

template <bool force_streaming>
OperatorStatus AutoPassThroughAggregateTransform<force_streaming>::transformImpl(Block & block)
{
    switch (status)
    {
    case Status::building_hash_map:
    {
        if unlikely (!block)
            status = Status::hash_map_done;
        else
            auto_pass_through_context->onBlock<force_streaming>(block);

        if (!auto_pass_through_context->passThroughBufferEmpty())
        {
            block = auto_pass_through_context->popPassThroughBuffer();
            return OperatorStatus::HAS_OUTPUT;
        }

        if unlikely (!block)
        {
            block = auto_pass_through_context->getData();
            return OperatorStatus::HAS_OUTPUT;
        }

        return OperatorStatus::NEED_INPUT;
    }
    default:
    {
        throw Exception(fmt::format("unexpected status: {}", magic_enum::enum_name(status)));
    }
    }
}

template <bool force_streaming>
OperatorStatus AutoPassThroughAggregateTransform<force_streaming>::tryOutputImpl(Block & block)
{
    if (!auto_pass_through_context->passThroughBufferEmpty())
    {
        block = auto_pass_through_context->popPassThroughBuffer();
        return OperatorStatus::HAS_OUTPUT;
    }

    switch (status)
    {
    case Status::building_hash_map:
    {
        return OperatorStatus::NEED_INPUT;
    }
    case Status::hash_map_done:
    {
        block = auto_pass_through_context->getData();
        return OperatorStatus::HAS_OUTPUT;
    }
    default:
    {
        throw Exception(fmt::format("unexpected status: {}", magic_enum::enum_name(status)));
    }
    }
}
} // namespace DB
