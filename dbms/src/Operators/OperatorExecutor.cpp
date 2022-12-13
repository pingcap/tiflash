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

#include <Operators/OperatorExecutor.h>

namespace DB
{
OperatorStatus OperatorExecutor::execute()
{
    auto [block, transform_index] = fetchBlock();
    for (; transform_index < transforms.size(); ++transform_index)
    {
        auto status = transforms[transform_index]->transform(block);
        switch (status)
        {
        case OperatorStatus::NEED_MORE:
        case OperatorStatus::FINISHED:
            return status;
        case OperatorStatus::MORE_OUTPUT:
            fetch_transform_stack.push_back(transform_index);
        case OperatorStatus::PASS:
            break;
        default:
            __builtin_unreachable();
        }
    }
    return sink->write(std::move(block));
}

std::tuple<Block, size_t> OperatorExecutor::fetchBlock()
{
    while (!fetch_transform_stack.empty())
    {
        auto index = fetch_transform_stack.back();
        Block block = transforms[index]->fetchBlock();
        if (block)
            return {std::move(block), index + 1};
        fetch_transform_stack.pop_back();
    }
    return {source->read(), 0};
}
} // namespace DB
