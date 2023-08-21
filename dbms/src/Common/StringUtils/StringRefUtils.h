// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/StringUtils/StringUtils.h>
#include <common/StringRef.h>

inline bool startsWith(const StringRef & view, const StringRef & prefix)
{
    return detail::startsWith(view.data, view.size, prefix.data, prefix.size);
}

// case insensitive version of startsWith
inline bool startsWithCI(const StringRef & view, const StringRef & prefix)
{
    return detail::startsWithCI(view.data, view.size, prefix.data, prefix.size);
}

inline bool endsWith(const StringRef & view, const char * prefix)
{
    return detail::endsWith(view.data, view.size, prefix, strlen(prefix)); //
}

// case insensitive version of endsWith
inline bool endsWithCI(const StringRef & view, const char * prefix)
{
    return detail::endsWithCI(view.data, view.size, prefix, strlen(prefix));
}

// n - number of characters to remove from the start of the view,
//     The behavior is undefined if `n > view.size`
inline StringRef removePrefix(const StringRef & view, size_t n)
{
    return StringRef{view.data + n, view.size - n};
}

// n - number of characters to remove from the end of the view,
//     The behavior is undefined if `n > view.size`
inline StringRef removeSuffix(const StringRef & view, size_t n)
{
    return StringRef{view.data, view.size - n};
}
