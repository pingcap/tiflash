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

#include <Common/Logger.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalFinalLimit.h>
#include <Interpreters/Context.h>
#include <Transforms/LimitSource.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalFinalLimit::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalFinalLimit::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalFinalLimit::getSampleBlock() const
{
    return sample_block;
}

void PhysicalFinalLimit::transform(TransformsPipeline & pipeline, Context &, size_t concurrency)
{
    pipeline.init(concurrency);
    pipeline.transform([&](auto & transforms) {
        transforms->setSource(std::make_shared<LimitSource>(limit_breaker));
    });
}
} // namespace DB
