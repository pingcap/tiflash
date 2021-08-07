#pragma once

#include <Common/TiFlashException.h>
#include <fmt/core.h>

namespace DB
{

inline void debugAssertImpl(bool predicate, const char * expression, const char * file_name, size_t line, const char * function_signature)
{
    if (!predicate)
    {
        throw TiFlashException(
            fmt::format("DEBUG_ASSERT: at {}:L{}, in function {}: \"{}\" failed, \"{}\"", file_name, line, function_signature, expression),
            Errors::Coprocessor::Internal);
    }
}

#ifndef NDEBUG

#define DEBUG_ASSERT(expression) debugAssertImpl((expression), #expression, __FILE__, __LINE__, __PRETTY_FUNCTION__)

#else

#define DEBUG_ASSERT(...)

#endif

} // namespace DB
