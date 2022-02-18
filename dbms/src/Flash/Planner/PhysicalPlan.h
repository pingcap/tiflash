#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Flash/Planner/PlanType.h>

#include <memory>

namespace DB
{
struct DAGPipeline;
class Context;
class DAGContext;

class PhysicalPlan;
using PhysicalPlanPtr = std::shared_ptr<PhysicalPlan>;

class PhysicalPlan
{
public:
    PhysicalPlan(const String & executor_id_, const PlanType & type_, const NamesAndTypes & schema_)
        : executor_id(executor_id_)
        , type(type_)
        , schema(schema_)
    {}

    virtual ~PhysicalPlan() = default;

    virtual PhysicalPlanPtr children(size_t) const = 0;

    virtual void setChild(size_t, const PhysicalPlanPtr &) = 0;

    const PlanType & tp() const { return type; }

    const String & execId() const { return executor_id; }

    const NamesAndTypes & getSchema() const { return schema; }

    virtual void appendChild(const PhysicalPlanPtr &) = 0;

    virtual size_t childrenSize() const = 0;

    virtual void transform(DAGPipeline & pipeline, const Context & context, size_t max_streams) = 0;

    virtual bool finalize(const Names & parent_require) = 0;
    void finalize() { finalize({}); }

    /// Obtain a sample block that contains the names and types of result columns.
    virtual const Block & getSampleBlock() const = 0;

    void disableRecordProfileStreams() { is_record_profile_streams = false; }

protected:
    void recordProfileStreams(DAGPipeline & pipeline, DAGContext & dag_context);

    String executor_id;
    PlanType type;
    NamesAndTypes schema;
    bool is_record_profile_streams = true;
};
} // namespace DB