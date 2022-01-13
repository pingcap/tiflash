#pragma once

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
class PlanBase;
using PlanPtr = std::shared_ptr<PlanBase>;

class PlanBase
{
public:
    virtual ~PlanBase() = default;

    virtual PlanPtr children(size_t) const = 0;

    virtual tipb::ExecType tp() const = 0;

    virtual void appendChild(const PlanPtr &) = 0;

    virtual size_t childrenSize() const = 0;

    template <typename PlanImpl, typename FF>
    void cast(FF && ff)
    {
        PlanImpl * impl_ptr = dynamic_cast<PlanImpl *>(this);
        assert(impl_ptr);
        ff(*impl_ptr);
    }
};

template <typename PlanImpl>
class Plan : public PlanBase
{
public:
    static constexpr size_t children_size = PlanImpl::children_size;

    Plan(const typename PlanImpl::Executor & executor_, String executor_id_)
        : executor(executor_)
        , executor_id(executor_id_)
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
            throw Exception("");
        }
        else if constexpr (1 == children_size)
        {
            if (left)
                throw Exception("");
            left = child;
        }
        else
        {
            if (left)
            {
                if (right)
                    throw Exception("");
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

    const typename PlanImpl::Executor & executor;
    const String executor_id;

private:
    PlanPtr left;
    PlanPtr right;
};
} // namespace DB
