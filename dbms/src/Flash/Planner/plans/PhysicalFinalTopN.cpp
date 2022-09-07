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
#include <Flash/Planner/plans/PhysicalFinalTopN.h>
#include <Interpreters/Context.h>
#include <Transforms/SortedSource.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalFinalTopN::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalFinalTopN::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalFinalTopN::getSampleBlock() const
{
    return sample_block;
}

void PhysicalFinalTopN::transform(TransformsPipeline & pipeline, Context &)
{
    sort_breaker->initForRead(sample_block);
    pipeline.transform([&](auto & transforms) {
        transforms->setSource(std::make_shared<SortedSource>(sort_breaker));
    });
}
} // namespace DB
