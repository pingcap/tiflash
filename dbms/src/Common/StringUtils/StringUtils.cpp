#include "StringUtils.h"

#include <cctype>

namespace detail
{

bool startsWith(const char * s, size_t size, const char * prefix, size_t prefix_size)
{
    return size >= prefix_size && 0 == memcmp(s, prefix, prefix_size);
}

bool endsWith(const char * s, size_t size, const char * suffix, size_t suffix_size)
{
    return size >= suffix_size && 0 == memcmp(s + size - suffix_size, suffix, suffix_size);
}

bool startsWithCI(const char * s, size_t size, const char * prefix, size_t prefix_size)
{
    if (size < prefix_size)
        return false;
    // case insensitive compare
    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (std::tolower(s[i]) != std::tolower(prefix[i]))
            return false;
    }
    return true;
}

bool endsWithCI(const char * s, size_t size, const char * suffix, size_t suffix_size)
{
    if (size < suffix_size)
        return false;
    // case insensitive compare
    for (size_t i = 0; i < suffix_size; ++i)
    {
        if (std::tolower(s[i]) != std::tolower(suffix[i]))
            return false;
    }
    return true;
}

} // namespace detail
