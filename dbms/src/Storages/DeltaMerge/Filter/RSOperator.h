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
using RSOperators   = std::vector<RSOperatorPtr>;
using Fields        = std::vector<Field>;

static const RSOperatorPtr EMPTY_FILTER{};

struct RSCheckParam
{
    ColumnIndexes indexes;
};


class RSOperator : public std::enable_shared_from_this<RSOperator>
{
protected:
    RSOperators children;

    RSOperator() = default;
    explicit RSOperator(const RSOperators & children_) : children(children_) {}

public:
    virtual ~RSOperator() = default;

    virtual String name()     = 0;
    virtual String toString() = 0;

    virtual RSResult roughCheck(const RSCheckParam & param) = 0;

    virtual RSOperatorPtr optimize() { return shared_from_this(); };
    virtual RSOperatorPtr switchDirection() { return shared_from_this(); };
    virtual RSOperatorPtr applyNot() = 0;
};

class ColCmpVal : public RSOperator
{
protected:
    Attr  attr;
    Field value;
    int   null_direction;

public:
    ColCmpVal(const Attr & attr_, const Field & value_, int null_direction_) : attr(attr_), value(value_), null_direction(null_direction_)
    {
    }

    String toString() override
    {
        return R"({"op":")" + name() +       //
            R"(","col":")" + attr.col_name + //
            R"(","value":")" + applyVisitor(FieldVisitorToString(), value) + "\"}";
    }
};


class LogicalOp : public RSOperator
{
public:
    explicit LogicalOp(const RSOperators & children_) : RSOperator(children_) {}

    String toString() override
    {
        String s = R"({"op":")" + name() + R"(","children":[)";
        for (auto & child : children)
            s += child->toString() + ",";
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


RSOperatorPtr createAnd(const RSOperators & children);
RSOperatorPtr createEqual(const Attr & attr, const Field & value);
RSOperatorPtr createGreater(const Attr & attr, const Field & value, int null_direction);
RSOperatorPtr createGreaterEqual(const Attr & attr, const Field & value, int null_direction);
RSOperatorPtr createIn(const Attr & attr, const Fields & values);
RSOperatorPtr createLess(const Attr & attr, const Field & value, int null_direction);
RSOperatorPtr createLessEqual(const Attr & attr, const Field & value, int null_direction);
RSOperatorPtr createLike(const Attr & attr, const Field & value);
RSOperatorPtr createNot(const RSOperatorPtr & op);
RSOperatorPtr createNotEqual(const Attr & attr, const Field & value);
RSOperatorPtr createNotIn(const Attr & attr, const Fields & values);
RSOperatorPtr createNotLike(const Attr & attr, const Field & values);
RSOperatorPtr createOr(const RSOperators & children);
RSOperatorPtr createUnsupported(const String & content, bool is_not);


} // namespace DM

} // namespace DB