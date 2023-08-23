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

#include <Common/Logger.h>
#include <Operators/Operator.h>

namespace DB
{
class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

// Wrap the BlockInputStream of pull model as the source operator of push model.
// Now it is used by `PhysicalMockExchangeReceiver/PhysicalMockTableScan` which are only used in unit test.
class BlockInputStreamSourceOp : public SourceOp
{
public:
    BlockInputStreamSourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const BlockInputStreamPtr & impl_);

    String getName() const override { return "BlockInputStreamSourceOp"; }

protected:
    void operatePrefixImpl() override;
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;

private:
    BlockInputStreamPtr impl;
};
} // namespace DB
