#include <Common/Exception.h>
#include <Flash/Planner/PlanType.h>

namespace DB
{
String toString(const PlanType & plan_type)
{
    switch (plan_type)
    {
    case Selection:
        return "Selection";
    case Aggregation:
        return "Aggregation";
    case Limit:
        return "Limit";
    case TopN:
        return "TopN";
    case Projection:
        return "Projection";
    case Source:
        return "Source";
    default:
        throw Exception("Unknown PlanType");
    }
}
} // namespace DB
