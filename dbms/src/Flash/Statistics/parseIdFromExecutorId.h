#pragma once

#include <common/types.h>

namespace DB
{
Int64 parseIdFromExecutorId(const String & executor_id);
} // namespace DB
