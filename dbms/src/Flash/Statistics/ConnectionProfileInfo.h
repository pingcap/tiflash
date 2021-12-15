#pragma once

#include <common/types.h>

#include <memory>

namespace DB
{
struct ConnectionProfileInfo
{
    size_t packets = 0;
    size_t bytes = 0;
};
} // namespace DB