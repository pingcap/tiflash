#include <Flash/Planner/PhysicalPlanHelper.h>

namespace DB::PhysicalPlanHelper
{
Names schemaToNames(const NamesAndTypes & schema)
{
    Names names;
    names.reserve(schema.size());
    for (const auto & column : schema)
        names.push_back(column.name);
    return names;
}
} // namespace DB::PhysicalPlanHelper
