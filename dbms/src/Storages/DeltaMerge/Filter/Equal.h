#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{
namespace DM
{
class Equal : public ColCmpVal
{
public:
    Equal(const Attr & attr_, const Field & value_)
        : ColCmpVal(attr_, value_, 0)
    {}

    String name() override { return "equal"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        return rsindex.minmax->checkEqual(pack_id, value, rsindex.type);
    }

    RSOperatorPtr applyNot() override { return createNotEqual(attr, value); };
};

} // namespace DM

} // namespace DB