#pragma once

#include <Core/NamesAndTypes.h>

namespace DB::PhysicalPlanHelper
{
Names schemaToNames(const NamesAndTypes & schema);
} // namespace DB::PhysicalPlanHelper
