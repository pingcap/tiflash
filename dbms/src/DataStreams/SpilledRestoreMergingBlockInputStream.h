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

#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Aggregator.h>

namespace DB
{
class SpilledRestoreMergingBlockInputStream : public IProfilingBlockInputStream
{
public:
    SpilledRestoreMergingBlockInputStream(Aggregator & aggregator_, bool is_final_, const String & req_id)
        : aggregator(aggregator_)
        , is_final(is_final_)
        , log(Logger::get(req_id))
    {
    }

    String getName() const override { return "SpilledRestoreMerging"; }

    Block getHeader() const override { return aggregator.getHeader(is_final); }

protected:
    Block readImpl() override
    {
        while (true)
        {
            Block out_block = popBlocksListFront(cur_block_list);
            if (out_block)
                return out_block;

            auto bucket_block_to_merge = aggregator.restoreBucketBlocks();
            if (bucket_block_to_merge.empty())
                return {};
            cur_block_list = aggregator.vstackBlocks(bucket_block_to_merge, is_final);
        }
    }

private:
    Aggregator & aggregator;
    bool is_final;
    const LoggerPtr log;

    BlocksList cur_block_list;
};
} // namespace DB
