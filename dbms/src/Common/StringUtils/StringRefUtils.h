#pragma once

#include <Common/StringUtils/StringUtils.h>
#include <common/StringRef.h>

inline bool startsWith(const StringRef & view, const StringRef & prefix)
{
    return detail::startsWith(view.data, view.size, prefix.data, prefix.size);
}

inline bool endsWith(const StringRef & view, const char * prefix) { return detail::endsWith(view.data, view.size, prefix, strlen(prefix)); }

// n - number of characters to remove from the start of the view,
//     The behavior is undefined if `n > view.size`
inline StringRef removePrefix(const StringRef & view, size_t n) { return StringRef{view.data + n, view.size - n}; }
