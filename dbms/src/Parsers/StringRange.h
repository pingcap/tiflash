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
#include <Parsers/TokenIterator.h>

#include <map>
#include <memory>


namespace DB
{

struct StringRange
{
    const char * first = nullptr;
    const char * second = nullptr;

    StringRange() {}
    StringRange(const char * begin, const char * end)
        : first(begin)
        , second(end)
    {}
    StringRange(TokenIterator token)
        : first(token->begin)
        , second(token->end)
    {}

    StringRange(TokenIterator token_begin, TokenIterator token_end)
    {
        /// Empty range.
        if (token_begin == token_end)
        {
            first = token_begin->begin;
            second = token_begin->begin;
            return;
        }

        TokenIterator token_last = token_end;
        --token_last;

        first = token_begin->begin;
        second = token_last->end;
    }
};

using StringPtr = std::shared_ptr<String>;


inline String toString(const StringRange & range)
{
    return range.first ? String(range.first, range.second) : String();
}

} // namespace DB
