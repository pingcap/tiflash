#pragma once

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Flash/Plan/PlanBase.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
template <typename PlanImpl>
class Plan : public PlanBase
{
public:
    static constexpr size_t children_size = PlanImpl::children_size;
    // tipb::executor.children().size() <= 2
    static_assert(children_size <= 2);

    Plan(const typename PlanImpl::Executor & executor_, String executor_id_)
        : PlanBase(executor_id_)
        , impl(executor_)
    {}

    PlanPtr children(size_t i) const override
    {
        assert(i < children_size);
        return i == 0 ? left : right;
    }

    void appendChild(const PlanPtr & child) override
    {
        if constexpr (0 == children_size)
        {
            throw TiFlashException("can't append child to plan that children_size == 0", Errors::Coprocessor::Internal);
        }
        else if constexpr (1 == children_size)
        {
            if (left)
                throw TiFlashException("can't append second child to plan that children_size == 1", Errors::Coprocessor::Internal);
            left = child;
        }
        else
        {
            if (left)
            {
                if (right)
                    throw TiFlashException("can't append third child to plan that children_size == 2", Errors::Coprocessor::Internal);
                right = child;
            }
            else
            {
                left = child;
            }
        }
    }

    tipb::ExecType tp() const override
    {
        return PlanImpl::tp();
    }

    size_t childrenSize() const override
    {
        return children_size;
    }

    const typename PlanImpl::Executor & impl;

private:
    PlanPtr left;
    PlanPtr right;
};
} // namespace DB
