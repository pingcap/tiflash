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
#include <DataStreams/IBlockInputStream.h>
#include <Operators/Operator.h>

namespace DB
{
// Used to read block from io-based block input stream, like `SpilledFilesInputStream`.
class IOBlockInputStreamSourceOp : public SourceOp
{
public:
    IOBlockInputStreamSourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const BlockInputStreamPtr & impl_)
        : SourceOp(exec_context_, req_id)
        , impl(impl_)
    {
        assert(impl);
        setHeader(impl->getHeader());
    }

    String getName() const override { return "IOBlockInputStreamSourceOp"; }

protected:
    void operatePrefixImpl() override { impl->readPrefix(); }

    void operateSuffixImpl() override { impl->readSuffix(); }

    OperatorStatus readImpl(Block & block) override
    {
        if (unlikely(is_done))
            return OperatorStatus::HAS_OUTPUT;
        if (!ret)
            return OperatorStatus::IO_IN;

        std::swap(ret, block);
        return OperatorStatus::HAS_OUTPUT;
    }

    OperatorStatus executeIOImpl() override
    {
        if unlikely (is_done || ret)
            return OperatorStatus::HAS_OUTPUT;

        ret = impl->read();
        if (unlikely(!ret))
            is_done = true;
        return OperatorStatus::HAS_OUTPUT;
    }

private:
    BlockInputStreamPtr impl;

    bool is_done = false;
    Block ret;
};
} // namespace DB
