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

#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>
#include <TiDB/Decode/JsonPathExpr.h>
#include <common/StringRef.h>
#include <common/memcpy.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

struct JsonPathLeg;
class JsonPathExpr;
struct JsonPathExprRef;
using JsonPathExprRefPtr = std::unique_ptr<JsonPathExprRef>;
using ConstJsonPathExprRawPtr = JsonPathExprRef const *;

class JsonPathExprRefContainer
{
public:
    explicit JsonPathExprRefContainer(JsonPathExprPtr source_);
    /// Return nullptr for empty JsonPathExpr, which parsed from '$', means extract all
    ConstJsonPathExprRawPtr firstRef() const
    {
        if (all_refs.empty())
            return nullptr;
        return all_refs.at(0).get();
    }
    std::pair<JsonPathLegRawPtr, ConstJsonPathExprRawPtr> pop(ConstJsonPathExprRawPtr path_ref_ptr);
    size_t size() const { return all_refs.size(); }

private:
    JsonPathExprPtr source;
    std::vector<JsonPathExprRefPtr> all_refs;
};
using JsonPathExprRefContainerPtr = std::unique_ptr<JsonPathExprRefContainer>;

/// Represent a view of JsonPathExpr[i:]
struct JsonPathExprRef
{
public:
    JsonPathExprRef(JsonPathExprRefContainer * container_, size_t leg_pos_, JsonPathExpressionFlag flag_);

    // popOneLeg returns a jsonPathLeg, and update itself without that leg
    std::pair<JsonPathLegRawPtr, ConstJsonPathExprRawPtr> popOneLeg() const;
    JsonPathExpressionFlag getFlag() const { return flag; }
    // CouldMatchMultipleValues returns true if pe contains any asterisk or range selection.
    bool couldMatchMultipleValues() const
    {
        return JsonPathExpr::containsAnyAsterisk(flag) || JsonPathExpr::containsAnyRange(flag);
    }

private:
    friend JsonPathExprRefContainer;
    JsonPathExprRefContainer * container;
    size_t leg_pos;
    JsonPathExpressionFlag flag = 0U;
};

std::vector<JsonPathExprRefContainerPtr> buildPathExprContainer(const StringRef & path);

} // namespace DB