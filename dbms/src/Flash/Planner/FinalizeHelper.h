#pragma once

#include <Interpreters/ExpressionActions.h>
#include <common/types.h>

namespace DB::FinalizeHelper
{
void prependProjectInputIfNeed(ExpressionActionsPtr & actions, size_t columns_from_previous);

void checkSchemaContainsParentRequire(const NamesAndTypes & schema, const Names & parent_require);

Names schemaToNames(const NamesAndTypes & schema);
} // namespace DB::FinalizeHelper
