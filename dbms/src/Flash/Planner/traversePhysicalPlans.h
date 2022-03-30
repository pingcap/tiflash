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

#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
/// f: (const PhysicalPlanPtr &)
template <typename FF>
void traverse(const PhysicalPlanPtr & plan, FF && f)
{
    f(plan);
    for (size_t i = 0; i < plan->childrenSize(); ++i)
    {
        traverse(plan->children(i), std::forward<FF>(f));
    }
}
} // namespace DB