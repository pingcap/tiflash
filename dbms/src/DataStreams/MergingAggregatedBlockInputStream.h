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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Aggregator.h>


namespace DB
{
/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedBlockInputStream : public IProfilingBlockInputStream
{
public:
    MergingAggregatedBlockInputStream(
        const BlockInputStreamPtr & input,
        const Aggregator::Params & params,
        bool final_,
        size_t max_threads_)
        : aggregator(params, /*req_id=*/"")
        , final(final_)
        , max_threads(max_threads_)
    {
        children.push_back(input);
    }

    String getName() const override { return "MergingAggregated"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    Aggregator aggregator;
    bool final;
    size_t max_threads;

    bool executed = false;
    BlocksList blocks;
    BlocksList::iterator it;
};

} // namespace DB
