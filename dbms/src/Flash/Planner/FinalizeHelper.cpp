#include <Common/FmtUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Interpreters/ExpressionActions.h>
#include <common/types.h>

namespace DB::FinalizeHelper
{
void prependProjectInputIfNeed(ExpressionActionsPtr & actions, size_t columns_from_previous)
{
    if (!actions->getRequiredColumnsWithTypes().empty()
        && columns_from_previous > actions->getRequiredColumnsWithTypes().size())
    {
        actions->prependProjectInput();
    }
}

void checkSchemaContainsParentRequire(const NamesAndTypes & schema, const Names & parent_require)
{
    NameSet schema_set;
    for (const auto & column : schema)
        schema_set.insert(column.name);
    for (const auto & parent_require_column : parent_require)
    {
        if (schema_set.find(parent_require_column) == schema_set.end())
            throw Exception(fmt::format("schema don't contain parent require column: {}", parent_require_column));
    }
}
} // namespace DB::FinalizeHelper