#pragma once
#include <Core/Types.h>

namespace DB::DM
{

// We can not define it inside class DMFile, or DMFile.h
// and ColumnStat.h need to include each other.
enum DMFileVersion : UInt32
{
    VERSION_BASE             = 0,
    VERSION_WITH_COLUMN_SIZE = 1,

    CURRENT_VERSION = VERSION_WITH_COLUMN_SIZE,
};

} // namespace DB::DM
