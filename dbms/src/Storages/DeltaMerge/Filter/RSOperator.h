#pragma once

#include <Common/FieldVisitors.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Index/RSIndex.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB
{
namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
using RSOperators = std::vector<RSOperatorPtr>;
using Fields = std::vector<Field>;

inline static const RSOperatorPtr EMPTY_FILTER{};

struct RSCheckParam
{
    ColumnIndexes indexes;
};


class RSOperator : public std::enable_shared_from_this<RSOperator>
{
protected:
    RSOperators children;

    RSOperator() = default;
    explicit RSOperator(const RSOperators & children_)
        : children(children_)
    {}

public:
    virtual ~RSOperator() = default;

    virtual String name() = 0;
    virtual String toDebugString() = 0;

    // TODO: implement a batch check version

    virtual RSResult roughCheck(size_t pack_id, const RSCheckParam & param) = 0;

    virtual Attrs getAttrs() = 0;

    virtual RSOperatorPtr optimize() { return shared_from_this(); };
    virtual RSOperatorPtr switchDirection() { return shared_from_this(); };
    virtual RSOperatorPtr applyNot() = 0;
};

class ColCmpVal : public RSOperator
{
protected:
    Attr attr;
    Field value;
    int null_direction;

public:
    ColCmpVal(const Attr & attr_, const Field & value_, int null_direction_)
        : attr(attr_)
        , value(value_)
        , null_direction(null_direction_)
    {
    }

    Attrs getAttrs() override { return {attr}; }

    String toDebugString() override
    {
        return R"({"op":")" + name() + //
            R"(","col":")" + attr.col_name + //
            R"(","value":")" + applyVisitor(FieldVisitorToDebugString(), value) + "\"}";
    }
};


class LogicalOp : public RSOperator
{
public:
    explicit LogicalOp(const RSOperators & children_)
        : RSOperator(children_)
    {}

    Attrs getAttrs() override
    {
        Attrs attrs;
        for (auto & child : children)
        {
            auto child_attrs = child->getAttrs();
            attrs.insert(attrs.end(), child_attrs.begin(), child_attrs.end());
        }
        return attrs;
    }

    String toDebugString() override
    {
        String s = R"({"op":")" + name() + R"(","children":[)";
        for (auto & child : children)
            s += child->toDebugString() + ",";
        s.pop_back();
        return s + "]}";
    }
};

#define GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex) \
    auto it = param.indexes.find(attr.col_id);                             \
    if (it == param.indexes.end())                                         \
        return Some;                                                       \
    auto rsindex = it->second;                                             \
    if (!rsindex.type->equals(*attr.type))                                 \
        return Some;


// logical
RSOperatorPtr createNot(const RSOperatorPtr & op);
RSOperatorPtr createOr(const RSOperators & children);
RSOperatorPtr createAnd(const RSOperators & children);
// compare
RSOperatorPtr createEqual(const Attr & attr, const Field & value);
RSOperatorPtr createNotEqual(const Attr & attr, const Field & value);
RSOperatorPtr createGreater(const Attr & attr, const Field & value, int null_direction);
RSOperatorPtr createGreaterEqual(const Attr & attr, const Field & value, int null_direction);
RSOperatorPtr createLess(const Attr & attr, const Field & value, int null_direction);
RSOperatorPtr createLessEqual(const Attr & attr, const Field & value, int null_direction);
// set
RSOperatorPtr createIn(const Attr & attr, const Fields & values);
RSOperatorPtr createNotIn(const Attr & attr, const Fields & values);
//
RSOperatorPtr createLike(const Attr & attr, const Field & value);
RSOperatorPtr createNotLike(const Attr & attr, const Field & values);
//
RSOperatorPtr createUnsupported(const String & content, const String & reason, bool is_not);


} // namespace DM

} // namespace DB
