#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ExpressionActions.h>
#include <common/types.h>

namespace DB::FinalizeHelper
{
void prependProjectInputIfNeed(ExpressionActionsPtr & actions, size_t columns_from_previous);

void checkSchemaContainsParentRequire(const NamesAndTypes & schema, const Names & parent_require);

void checkParentRequireContainsSchema(const Names & parent_require, const NamesAndTypes & schema);

void checkSampleBlockContainsSchema(const Block & sample_block, const NamesAndTypes & schema);

void checkSchemaContainsSampleBlock(const NamesAndTypes & schema, const Block & sample_block);
} // namespace DB::FinalizeHelper
