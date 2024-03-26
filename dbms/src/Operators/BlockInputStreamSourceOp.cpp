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

#include <DataStreams/IBlockInputStream.h>
#include <Operators/BlockInputStreamSourceOp.h>

namespace DB
{
BlockInputStreamSourceOp::BlockInputStreamSourceOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const BlockInputStreamPtr & impl_)
    : SourceOp(exec_context_, req_id)
    , impl(impl_)
{
    assert(impl);
    setHeader(impl->getHeader());
}

void BlockInputStreamSourceOp::operatePrefixImpl()
{
    impl->readPrefix();
}

void BlockInputStreamSourceOp::operateSuffixImpl()
{
    impl->readSuffix();
}

OperatorStatus BlockInputStreamSourceOp::readImpl(Block & block)
{
    block = impl->read();
    return OperatorStatus::HAS_OUTPUT;
}
} // namespace DB
