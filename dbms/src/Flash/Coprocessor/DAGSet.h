#pragma once

#include <tipb/expression.pb.h>
#include <memory>
#include <utility>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

struct DAGSet
{
    DAGSet(SetPtr constant_set_, std::vector<const tipb::Expr *> remaining_exprs_)
        : constant_set(std::move(constant_set_)), remaining_exprs(std::move(remaining_exprs_)){};
    SetPtr constant_set;
    std::vector<const tipb::Expr *> remaining_exprs;
};
} // namespace DB
