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

#include <Core/SortDescription.h>
#include <Flash/Planner/plans/PhysicalUnary.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalWindowSort : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Sort & window_sort,
        const PhysicalPlanPtr & child);

    PhysicalWindowSort(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanPtr & child_)
        : PhysicalUnary(executor_id_, PlanType::WindowSort, schema_, req_id, child_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

private:
    SortDescription order_descr;
};
} // namespace DB
