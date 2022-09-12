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

#include <Flash/Planner/plans/PhysicalPartialLimit.h>
#include <Interpreters/Context.h>
#include <Transforms/LimitSink.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalPartialLimit::transformImpl(DAGPipeline &, Context &, size_t)
{
    throw Exception("Unsupport");
}

void PhysicalPartialLimit::finalize(const Names & parent_require)
{
    child->finalize(parent_require);
}

const Block & PhysicalPartialLimit::getSampleBlock() const
{
    return child->getSampleBlock();
}

void PhysicalPartialLimit::transform(TransformsPipeline & pipeline, Context & context, size_t concurrency)
{
    child->transform(pipeline, context, concurrency);

    pipeline.transform([&](auto & transforms) {
        transforms->setSink(std::make_shared<LimitSink>(limit_breaker));
    });
    limit_breaker->initForRead(pipeline.getHeader());
}
} // namespace DB
