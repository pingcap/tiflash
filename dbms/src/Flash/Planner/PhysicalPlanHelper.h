#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

namespace DB::PhysicalPlanHelper
{
Names schemaToNames(const NamesAndTypes & schema);

ExpressionActionsPtr newActions(const Block & input_block, const Context & context);

ExpressionActionsPtr newActions(const NamesAndTypes & input_columns, const Context & context);

Block constructBlockFromSchema(const NamesAndTypes & schema);
} // namespace DB::PhysicalPlanHelper