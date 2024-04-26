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

#include <Common/FieldVisitors.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Index/RSIndex.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB::DM
{

class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
using RSOperators = std::vector<RSOperatorPtr>;
using Fields = std::vector<Field>;

inline static const RSOperatorPtr EMPTY_RS_OPERATOR{};

struct RSCheckParam
{
    ColumnIndexes indexes;
};

class RSOperator
{
protected:
    RSOperator() = default;

public:
    virtual ~RSOperator() = default;

    virtual String name() = 0;
    virtual String toDebugString() = 0;

    virtual RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) = 0;

    virtual ColIds getColumnIDs() = 0;
};

class ColCmpVal : public RSOperator
{
protected:
    Attr attr;
    Field value;

public:
    ColCmpVal(const Attr & attr_, const Field & value_)
        : attr(attr_)
        , value(value_)
    {}

    ColIds getColumnIDs() override { return {attr.col_id}; }

    String toDebugString() override
    {
        return fmt::format(
            R"({{"op":"{}","col":"{}","value":"{}"}})",
            name(),
            attr.col_name,
            applyVisitor(FieldVisitorToDebugString(), value));
    }
};


class LogicalOp : public RSOperator
{
protected:
    RSOperators children;

public:
    explicit LogicalOp(const RSOperators & children_)
        : children(children_)
    {}

    ColIds getColumnIDs() override
    {
        ColIds col_ids;
        for (const auto & child : children)
        {
            auto child_col_ids = child->getColumnIDs();
            col_ids.insert(col_ids.end(), child_col_ids.begin(), child_col_ids.end());
        }
        return col_ids;
    }

    String toDebugString() override
    {
        FmtBuffer buf;
        buf.fmtAppend(R"({{"op":"{}","children":[)", name());
        buf.joinStr(
            children.cbegin(),
            children.cend(),
            [](const auto & child, FmtBuffer & fb) { fb.append(child->toDebugString()); },
            ",");
        buf.append("]}");
        return buf.toString();
    }
};

inline std::optional<RSIndex> getRSIndex(const RSCheckParam & param, const Attr & attr)
{
    auto it = param.indexes.find(attr.col_id);
    if (it != param.indexes.end() && it->second.type->equals(*attr.type))
    {
        return it->second;
    }
    return std::nullopt;
}

template <typename Op>
RSResults minMaxCheckCmp(
    size_t start_pack,
    size_t pack_count,
    const RSCheckParam & param,
    const Attr & attr,
    const Field & value)
{
    auto rs_index = getRSIndex(param, attr);
    return rs_index ? rs_index->minmax->checkCmp<Op>(start_pack, pack_count, value, rs_index->type)
                    : RSResults(pack_count, RSResult::Some);
}

// logical
RSOperatorPtr createNot(const RSOperatorPtr & op);
RSOperatorPtr createOr(const RSOperators & children);
RSOperatorPtr createAnd(const RSOperators & children);
// compare
RSOperatorPtr createEqual(const Attr & attr, const Field & value);
RSOperatorPtr createNotEqual(const Attr & attr, const Field & value);
RSOperatorPtr createGreater(const Attr & attr, const Field & value);
RSOperatorPtr createGreaterEqual(const Attr & attr, const Field & value);
RSOperatorPtr createLess(const Attr & attr, const Field & value);
RSOperatorPtr createLessEqual(const Attr & attr, const Field & value);
// set
RSOperatorPtr createIn(const Attr & attr, const Fields & values);
//
RSOperatorPtr createLike(const Attr & attr, const Field & value);
//
RSOperatorPtr createIsNull(const Attr & attr);
//
RSOperatorPtr createUnsupported(const String & content, const String & reason);

} // namespace DB::DM
