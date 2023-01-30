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

#pragma once

#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Planner/Plans/PhysicalUnary.h>
#include <Interpreters/WindowDescription.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalWindow : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Window & window,
        const FineGrainedShuffle & fine_grained_shuffle,
        const PhysicalPlanNodePtr & child);

    PhysicalWindow(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const WindowDescription & window_description_,
        const FineGrainedShuffle & fine_grained_shuffle_)
        : PhysicalUnary(executor_id_, PlanType::Window, schema_, req_id, child_)
        , window_description(window_description_)
        , fine_grained_shuffle(fine_grained_shuffle_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

private:
    WindowDescription window_description;
    FineGrainedShuffle fine_grained_shuffle;
};
} // namespace DB
