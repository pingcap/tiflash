#pragma once

#include <common/types.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
class PlanBase;
using PlanPtr = std::shared_ptr<PlanBase>;

class PlanBase
{
public:
    explicit PlanBase(const String & executor_id_)
        : executor_id(executor_id_)
    {}

    virtual ~PlanBase() = default;

    virtual PlanPtr children(size_t) const = 0;

    virtual tipb::ExecType tp() const = 0;

    virtual void appendChild(const PlanPtr &) = 0;

    virtual size_t childrenSize() const = 0;

    template <typename PlanImpl, typename FF>
    void toImpl(FF && ff)
    {
        PlanImpl * impl_ptr = dynamic_cast<PlanImpl *>(this);
        assert(impl_ptr);
        ff(*impl_ptr);
    }

    const String executor_id;
};
} // namespace DB
