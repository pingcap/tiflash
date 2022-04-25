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
    static PhysicalPlanPtr build(
        const Block & sample_block,
        const String & req_id)
    {
        NamesAndTypes schema;
        for (const auto & col : sample_block)
            schema.emplace_back(col.name, col.type);
        return std::make_shared<PhysicalSource>("source", schema, sample_block, req_id);
    }

    PhysicalSource(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const Block & sample_block_,
        const String & req_id)
        : PhysicalLeaf(executor_id_, PlanType::Source, schema_, req_id)
        , sample_block(sample_block_)
    {
        is_record_profile_streams = false;
    }

    void transformImpl(DAGPipeline &, Context &, size_t) override {}

    void finalize(const Names &) override {}

    const Block & getSampleBlock() const override { return sample_block; }

private:
    Block sample_block;
};
} // namespace DB
