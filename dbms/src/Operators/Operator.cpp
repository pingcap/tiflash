// Copyright 2023 PingCAP, Ltd.
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

#include <Operators/Operator.h>

namespace DB
{
OperatorStatus Operator::await()
{
    // TODO collect operator profile info here.
    return awaitImpl();
}

OperatorStatus Operator::spill()
{
    // TODO collect operator profile info here.
    return spillImpl();
}

OperatorStatus SourceOp::read(Block & block)
{
    // TODO collect operator profile info here.
    assert(!block);
    auto status = readImpl(block);
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
#endif
    return status;
}

OperatorStatus TransformOp::transform(Block & block)
{
    // TODO collect operator profile info here.
    auto status = transformImpl(block);
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
#endif
    return status;
}

OperatorStatus TransformOp::tryOutput(Block & block)
{
    // TODO collect operator profile info here.
    assert(!block);
    auto status = tryOutputImpl(block);
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
#endif
    return status;
}

OperatorStatus SinkOp::write(Block && block)
{
#ifndef NDEBUG
    if (block)
    {
        Block header = getHeader();
        assertBlocksHaveEqualStructure(block, header, getName());
    }
#endif
    // TODO collect operator profile info here.
    return writeImpl(std::move(block));
}
} // namespace DB
