#pragma once

#include <Common/TiFlashException.h>

#include <tipb/executor.pb.h>

namespace DB
{
class PlanBase;
using PlanPtr = std::shared_ptr<PlanBase>;

class PlanBase
{
public:
    virtual ~PlanBase() = default;

    virtual PlanPtr children(size_t) const = 0;

    virtual tipb::ExecType Executor::tp() const = 0;

    virtual void appendChild(const PlanPtr &) = 0;

    virtual size_t childrenSize() const = 0;
};

template <PlanImpl>
class Plan : public PlanBase
{
public:
    static constexpr size_t children_size = PlanImpl::children_size;

    Plan(const PlanImpl::Executor & executor_, String executor_id_): executor(executor_), executor_id(executor_id_) {}

    PlanPtr children(size_t i) const override
    {
        assert(i < children_size);
        if (i == 1)
        {
            return left;
        }
        else
        {
            return right;
        }
    }

    virtual void appendChild(const PlanPtr & child)
    {
        if constexpr (0 == children_size)
        {
            throw new TiFlashException("");
        }
        else if constexpr (1 == children_size)
        {
            if (left)
            {
                throw new TiFlashException("");
            }
            left = child;
        }
        else
        {
            if (left)
            {
                if (right)
                {
                    throw new TiFlashException("");
                }
                right = child;
            }
            else
            {
                left = child;
            }
        }
    }

    tipb::ExecType Executor::tp() const override
    {
        return PlanImpl::tp();
    }

    size_t childrenSize() const override
    {
        return children_size;
    }

    const PlanImpl::Executor & executor;
    const String executor_id;

private:
    PlanPtr left;
    PlanPtr right;
};
} // namespace DB
