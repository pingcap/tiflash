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

#include <Common/FmtUtils.h>
#include <Flash/Planner/PhysicalPlanVisitor.h>

namespace DB::PhysicalPlanVisitor
{
namespace
{
void addPrefix(FmtBuffer & buffer, size_t level)
{
    buffer.append(String(level, ' '));
}

void doVisitToString(FmtBuffer & buffer, const PhysicalPlanNodePtr & physical_plan, size_t level)
{
    visit(physical_plan, [&buffer, &level](const PhysicalPlanNodePtr & plan) {
        RUNTIME_CHECK(plan);
        addPrefix(buffer, level);
        buffer.fmtAppend("{}\n", plan->toString());
        ++level;
        if (plan->childrenSize() <= 1)
        {
            return true;
        }
        else
        {
            for (size_t i = 0; i < plan->childrenSize(); ++i)
                doVisitToString(buffer, plan->children(i), level);
            return false;
        }
    });
}
} // namespace

String visitToString(const PhysicalPlanNodePtr & plan)
{
    FmtBuffer buffer;
    doVisitToString(buffer, plan, 0);
    return buffer.toString();
}
} // namespace DB::PhysicalPlanVisitor
