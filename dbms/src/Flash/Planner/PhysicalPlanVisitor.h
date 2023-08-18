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

#include <Flash/Planner/PhysicalPlanNode.h>

namespace DB::PhysicalPlanVisitor
{
/// visit physical plan node tree and apply function.
/// f: (const PhysicalPlanNodePtr &) -> bool, return true to continue visit.
template <typename FF>
void visit(const PhysicalPlanNodePtr & plan, FF && f)
{
    if (f(plan))
    {
        for (size_t i = 0; i < plan->childrenSize(); ++i)
        {
            DB::PhysicalPlanVisitor::visit(plan->children(i), std::forward<FF>(f));
        }
    }
}

/// visit physical plan node tree in reverse order and apply function.
/// f: (const PhysicalPlanNodePtr &).
template <typename FF>
void visitPostOrder(const PhysicalPlanNodePtr & plan, FF && f)
{
    for (size_t i = 0; i < plan->childrenSize(); ++i)
    {
        visitPostOrder(plan->children(i), std::forward<FF>(f));
    }
    f(plan);
}

String visitToString(const PhysicalPlanNodePtr & plan);
} // namespace DB::PhysicalPlanVisitor
