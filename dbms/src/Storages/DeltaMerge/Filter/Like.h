#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class Like : public ColCmpVal
{
public:
    Like(const Attr & attr_, const Field & value_) : ColCmpVal(attr_, value_, 0) {}

    String name() override { return "like"; }

    RSResult roughCheck(size_t /*pack_id*/, const RSCheckParam & /*param*/) override { return Some; }

    RSOperatorPtr applyNot() override { return createNotLike(attr, value); };
};

} // namespace DM

} // namespace DB