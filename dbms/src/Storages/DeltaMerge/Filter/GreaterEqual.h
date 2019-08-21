#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class GreaterEqual : public ColCmpVal
{
public:
    GreaterEqual(const Attr & attr_, const Field & value_, int null_direction) : ColCmpVal(attr_, value_, null_direction) {}

    String name() override { return "greater_equal"; }

    RSResult roughCheck(const RSCheckParam & param) override
    {
//        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        auto it = param.indexes.find(attr.col_id);
    if (it == param.indexes.end())
        return Some;
    auto rsindex = it->second;
    if (!rsindex.type->equals(*attr.type))
        return Some;
        return rsindex.minmax->checkGreaterEqual(value, rsindex.type, null_direction);
    }

    RSOperatorPtr applyNot() override { return createLess(attr, value, null_direction); };
    RSOperatorPtr switchDirection() override { return createLessEqual(attr, value, null_direction); }
};

} // namespace DM

} // namespace DB