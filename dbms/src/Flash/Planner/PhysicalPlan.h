#pragma once

#include <Flash/Planner/PlanType.h>

#include <memory>

namespace DB
{
struct DAGPipeline;
class Context;

class PhysicalPlan;
using PhysicalPlanPtr = std::shared_ptr<PhysicalPlan>;

class PhysicalPlan
{
public:
    PhysicalPlan(const String & executor_id_, const PlanType & type_, const Names & schema_)
        : executor_id(executor_id_)
        , type(type_)
        , schema(schema_)
    {}

    virtual ~PhysicalPlan() = default;

    virtual PhysicalPlanPtr children(size_t) const = 0;

    virtual void setChild(size_t, const PhysicalPlanPtr &) = 0;

    const PlanType & tp() const { return type; }

    const String & executorId() const { return executor_id; }

    virtual void appendChild(const PhysicalPlanPtr &) = 0;

    virtual size_t childrenSize() const = 0;

    virtual void transform(DAGPipeline & pipeline, Context & context, size_t max_streams) = 0;

    virtual bool finalize(const Names & parent_require) = 0;
    void finalize() { finalize({}); }

    /// Obtain a sample block that contains the names and types of result columns.
    virtual const Block & getSampleBlock() const;

protected:
    String executor_id;
    PlanType type;
    Names schema;
};
} // namespace DB