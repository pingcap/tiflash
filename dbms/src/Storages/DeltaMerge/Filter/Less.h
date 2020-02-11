#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class Less : public ColCmpVal
{
public:
    Less(const Attr & attr_, const Field & value_, int null_direction) : ColCmpVal(attr_, value_, null_direction) {}

    String name() override { return "less"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        return !rsindex.minmax->checkGreaterEqual(pack_id, value, rsindex.type, null_direction);
    }

    RSOperatorPtr applyNot() override { return createGreaterEqual(attr, value, null_direction); };
    RSOperatorPtr switchDirection() override { return createGreater(attr, value, null_direction); }
};

} // namespace DM

} // namespace DB