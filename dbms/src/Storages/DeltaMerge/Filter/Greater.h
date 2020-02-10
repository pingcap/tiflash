#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class Greater : public ColCmpVal
{
public:
    Greater(const Attr & attr_, const Field & value_, int null_direction_) : ColCmpVal(attr_, value_, null_direction_) {}

    String name() override { return "greater"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        return rsindex.minmax->checkGreater(pack_id, value, rsindex.type, null_direction);
    }

    RSOperatorPtr applyNot() override { return createLessEqual(attr, value, null_direction); };
    RSOperatorPtr switchDirection() override { return createLess(attr, value, null_direction); }
};

} // namespace DM

} // namespace DB