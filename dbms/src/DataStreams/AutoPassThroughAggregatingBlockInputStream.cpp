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

#include <DataStreams/AutoPassThroughAggregatingBlockInputStream.h>

namespace DB
{

Block AutoPassThroughAggregatingBlockInputStream::readImpl()
{
    while (!build_done)
    {
        Block block = children[0]->read();
        if (block)
        {
            auto_pass_through_context->onBlock(block);
        }
        else
        {
            build_done = true;
            break;
        }

        if (!auto_pass_through_context->passThroughBufferEmpty())
            return checkSelective(auto_pass_through_context->popPassThroughBuffer());
    }

    assert(build_done);
    if (!auto_pass_through_context->passThroughBufferEmpty())
        return checkSelective(auto_pass_through_context->popPassThroughBuffer());

    return checkSelective(auto_pass_through_context->getData());
}

} // namespace DB
