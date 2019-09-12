#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class Not : public LogicalOp
{
public:
    Not(const RSOperatorPtr & child) : LogicalOp({child}) {}

    String name() override { return "not"; }

    RSResult roughCheck(const RSCheckParam & param) override { return !children[0]->roughCheck(param); }

    RSOperatorPtr applyNot() override { return children[0]; };
};

} // namespace DM

} // namespace DB