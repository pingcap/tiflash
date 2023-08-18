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

#include <Common/Exception.h>
#include <Storages/Transaction/JsonBinary.h>
#include <Storages/Transaction/JsonPathExpr.h>
#include <Storages/Transaction/JsonPathExprRef.h>

namespace DB
{
JsonPathExprRefContainer::JsonPathExprRefContainer(JsonPathExprPtr source_)
    : source(source_)
{
    auto leg_count = source->getLegs().size();
    all_refs.resize(leg_count);
    /// calculate all possible flags from back to front
    JsonPathExpressionFlag new_flag = 0;
    for (Int32 i = leg_count - 1; i >= 0; --i)
    {
        const auto & leg = source->getLegs()[i];
        if (leg->type == JsonPathLeg::JsonPathLegArraySelection)
        {
            switch (leg->array_selection.type)
            {
            case JsonPathArraySelectionAsterisk:
                new_flag |= JsonPathExpr::JsonPathExpressionContainsAsterisk;
                break;
            case JsonPathArraySelectionRange:
                new_flag |= JsonPathExpr::JsonPathExpressionContainsRange;
                break;
            default:
                break;
            }
        }
        else if (leg->type == JsonPathLeg::JsonPathLegKey && leg->dot_key.key == "*")
        {
            new_flag |= JsonPathExpr::JsonPathExpressionContainsAsterisk;
        }
        else if (leg->type == JsonPathLeg::JsonPathLegDoubleAsterisk)
        {
            new_flag |= JsonPathExpr::JsonPathExpressionContainsDoubleAsterisk;
        }
        all_refs[i] = std::make_unique<JsonPathExprRef>(this, i, new_flag);
    }
}

std::pair<JsonPathLegRawPtr, ConstJsonPathExprRawPtr> JsonPathExprRefContainer::pop(ConstJsonPathExprRawPtr path_ref_ptr)
{
    RUNTIME_CHECK(path_ref_ptr);
    auto leg_pos = path_ref_ptr->leg_pos;
    auto new_leg_pos = leg_pos + 1;
    return std::make_pair(source->getLegs()[leg_pos].get(), new_leg_pos < all_refs.size() ? all_refs[new_leg_pos].get() : nullptr);
}

JsonPathExprRef::JsonPathExprRef(JsonPathExprRefContainer * container_, size_t leg_pos_, JsonPathExpressionFlag flag_)
    : container(container_)
    , leg_pos(leg_pos_)
    , flag(flag_)
{}

std::pair<JsonPathLegRawPtr, ConstJsonPathExprRawPtr> JsonPathExprRef::popOneLeg() const
{
    return container->pop(this);
}
} // namespace DB
