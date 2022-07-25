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

#include <Core/Block.h>
#include <Flash/Planner/plans/PhysicalLeaf.h>

namespace DB
{
class PhysicalSource : public PhysicalLeaf
{
public:
    static PhysicalPlanNodePtr build(
        const String & executor_id,
        const BlockInputStreams & source_streams,
        const LoggerPtr & log);

    PhysicalSource(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const Block & sample_block_,
        const BlockInputStreams & source_streams_)
        : PhysicalLeaf(executor_id_, PlanType::Source, schema_, req_id)
        , sample_block(sample_block_)
        , source_streams(source_streams_)
    {
        is_record_profile_streams = false;
    }

    void transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/) override;

    void finalize(const Names &) override {}

    const Block & getSampleBlock() const override { return sample_block; }

private:
    Block sample_block;

    BlockInputStreams source_streams;
};
} // namespace DB
