#include "StringUtils.h"

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

} // namespace detail
