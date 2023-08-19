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

#include <Core/Types.h>

namespace DB
{
/// Transforms the LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
inline String likePatternToRegexp(const String & pattern)
{
    String res;
    res.reserve(pattern.size() * 2);
    const char * pos = pattern.data();
    const char * end = pos + pattern.size();

    if (pos < end && *pos == '%')
        ++pos;
    else
        res = "^";

    while (pos < end)
    {
        switch (*pos)
        {
        case '^':
        case '$':
        case '.':
        case '[':
        case '|':
        case '(':
        case ')':
        case '?':
        case '*':
        case '+':
        case '{':
            res += '\\';
            res += *pos;
            break;
        case '%':
            if (pos + 1 != end)
                res += ".*";
            else
                return res;
            break;
        case '_':
            res += ".";
            break;
        case '\\':
            ++pos;
            if (pos == end)
                res += "\\\\";
            else
            {
                if (*pos == '%' || *pos == '_')
                    res += *pos;
                else
                {
                    res += '\\';
                    res += *pos;
                }
            }
            break;
        default:
            res += *pos;
            break;
        }
        ++pos;
    }

    res += '$';
    return res;
}

} // namespace DB
