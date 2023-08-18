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
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Operators/Operator.h>
#include <pingcap/coprocessor/Client.h>

namespace DB
{

class CoprocessorReaderSourceOp : public SourceOp
{
public:
    CoprocessorReaderSourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        CoprocessorReaderPtr coprocessor_reader_);

    String getName() const override;

protected:
    void operatePrefixImpl() override;
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;
    OperatorStatus awaitImpl() override;

    IOProfileInfoPtr getIOProfileInfo() const override { return io_profile_info; }

private:
    Block popFromBlockQueue();

private:
    CoprocessorReaderPtr coprocessor_reader;
    UInt64 total_rows{};
    std::unique_ptr<CHBlockChunkDecodeAndSquash> decoder_ptr;
    std::queue<Block> block_queue;
    std::optional<std::pair<pingcap::coprocessor::ResponseIter::Result, bool>> reader_res;

    IOProfileInfoPtr io_profile_info;
};
} // namespace DB
