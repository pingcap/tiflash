// Copyright 2022 PingCAP, Ltd.
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

#include <common/StringRef.h>
#include <fmt/core.h>
#include <fmt/format.h>

namespace DB
{
class FmtBuffer
{
public:
    FmtBuffer() = default;

    template <typename... Args>
    FmtBuffer & fmtAppend(Args &&... args)
    {
        fmt::format_to(std::back_inserter(buffer), std::forward<Args>(args)...);
        return *this;
    }

    FmtBuffer & append(StringRef s)
    {
        buffer.append(s.data, s.data + s.size);
        return *this;
    }

    std::string toString() const
    {
        return fmt::to_string(buffer);
    }

    template <typename Iter>
    FmtBuffer & joinStr(
        Iter first,
        Iter end)
    {
        return joinStr(first, end, ", ");
    }

    template <typename Iter>
    FmtBuffer & joinStr(
        Iter first,
        Iter end,
        StringRef delimiter)
    {
        auto func = [](const auto & s, FmtBuffer & fb) {
            fb.append(s);
        };
        return joinStr(first, end, func, delimiter);
    }

    template <typename Iter, typename FF>
    FmtBuffer & joinStr(
        Iter first,
        Iter end,
        FF && toStringFunc, // void (const auto &, FmtBuffer &)
        StringRef delimiter)
    {
        if (first == end)
            return *this;
        toStringFunc(*first, *this);
        ++first;
        for (; first != end; ++first)
        {
            append(delimiter);
            toStringFunc(*first, *this);
        }
        return *this;
    }

    void resize(size_t count) { buffer.resize(count); }
    void reserve(size_t capacity) { buffer.reserve(capacity); }
    void clear() { buffer.clear(); }

private:
    fmt::memory_buffer buffer;
};
} // namespace DB
