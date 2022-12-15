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
    Block block;
    auto [status, transform_index] = fetchBlock(block);
    if (status != OperatorStatus::PASS)
        return status;

    assert(transform_index >= 0);
    for (size_t i = transform_index; i < transforms.size(); ++i)
    {
        auto status = transforms[i]->transform(block);
        if (status != OperatorStatus::PASS)
            return pushSpiller(status, transforms[i]);
    }
    return pushSpiller(sink->write(std::move(block)), sink);
}

std::tuple<OperatorStatus, int64_t> OperatorExecutor::fetchBlock(Block & block)
{
    auto status = sink->prepare();
    if (status != OperatorStatus::PASS)
        return {pushSpiller(status, sink), -1};
    for (int64_t index = transforms.size() - 1; index >= 0; --index)
    {
        auto status = transforms[index]->fetchBlock(block);
        if (status != OperatorStatus::NO_OUTPUT)
            return {pushSpiller(status, transforms[index]), index + 1};
    }
    return {pushSpiller(source->read(block), source), 0};
}

OperatorStatus OperatorExecutor::await()
{
    auto status = sink->await();
    if (status != OperatorStatus::PASS)
        return status;
    for (auto it = transforms.rbegin(); it != transforms.rend(); ++it)
    {
        auto status = (*it)->await();
        if (status != OperatorStatus::SKIP)
            return status;
    }
    return source->await();
}

OperatorStatus OperatorExecutor::spill()
{
    assert(spiller);
    auto status = (*spiller)->spill();
    if (status != OperatorStatus::SPILLING)
        spiller.reset();
    return status;
}
} // namespace DB
