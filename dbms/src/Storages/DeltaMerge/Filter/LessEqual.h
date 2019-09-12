#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class LessEqual : public ColCmpVal
{
public:
    LessEqual(const Attr & attr_, const Field & value_, int null_direction) : ColCmpVal(attr_, value_, null_direction) {}

    String name() override { return "less_equal"; }

    RSResult roughCheck(const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        return !rsindex.minmax->checkGreater(value, rsindex.type, null_direction);
    }

    RSOperatorPtr applyNot() override { return createGreater(attr, value, null_direction); };
    RSOperatorPtr switchDirection() override { return createGreater(attr, value, null_direction); }
};

} // namespace DM

} // namespace DB