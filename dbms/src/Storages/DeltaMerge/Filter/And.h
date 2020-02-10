#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class And : public LogicalOp
{
public:
    explicit And(const RSOperators & children_) : LogicalOp(children_)
    {
        if (children.empty())
            throw Exception("Unexpected empty children");
    }

    String name() override { return "and"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        auto res = children[0]->roughCheck(pack_id, param);
        if (res == None)
            return res;
        for (size_t i = 1; i < children.size(); ++i)
        {
            res = res && children[i]->roughCheck(pack_id, param);
            if (res == None)
                return res;
        }
        return res;
    }

    RSOperatorPtr applyNot() override
    {
        RSOperators new_children;
        for (auto & child : children)
            new_children.push_back(child->applyNot());
        return createOr(new_children);
    };

    // TODO: override applyOptimize()
};

} // namespace DM

} // namespace DB