#pragma once

#include "Common.h"

extern "C" {

namespace DB
{

enum class ColumnFamilyType : uint8_t
{
    Lock = 0,
    Write,
    Default,
};
}
}
