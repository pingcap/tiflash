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

#include <Common/RedactHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormat.h>

namespace DB
{
class UniversalPageId final
{
public:
    static inline UniversalPageId toFullUniversalPageId(const String & prefix, PageIdU64 page_id)
    {
        WriteBufferFromOwnString buff;
        writeString(prefix, buff);
        UniversalPageIdFormat::encodeUInt64(page_id, buff);
        return buff.releaseStr();
    }

public:
    UniversalPageId() = default;

    UniversalPageId(String id_) // NOLINT(google-explicit-constructor)
        : id(std::move(id_))
    {}
    UniversalPageId(const char * id_) // NOLINT(google-explicit-constructor)
        : id(id_)
    {}
    UniversalPageId(const char * id_, size_t sz_)
        : id(id_, sz_)
    {}

    UniversalPageId & operator=(String && id_) noexcept
    {
        id.swap(id_);
        return *this;
    }
    bool operator==(const UniversalPageId & rhs) const noexcept
    {
        return id == rhs.id;
    }
    bool operator!=(const UniversalPageId & rhs) const noexcept
    {
        return id != rhs.id;
    }
    bool operator>=(const UniversalPageId & rhs) const noexcept
    {
        return id >= rhs.id;
    }
    size_t rfind(const String & str, size_t pos) const noexcept
    {
        return id.rfind(str, pos);
    }

    const char * data() const { return id.data(); }
    size_t size() const { return id.size(); }
    bool empty() const { return id.empty(); }
    UniversalPageId substr(size_t pos, size_t npos) const { return id.substr(pos, npos); }
    bool operator<(const UniversalPageId & rhs) const { return id < rhs.id; }

    String toStr() const { return id; }
    const String & asStr() const { return id; }

    friend bool operator==(const String & lhs, const UniversalPageId & rhs);

private:
    String id;
};
using UniversalPageIds = std::vector<UniversalPageId>;

inline bool operator==(const String & lhs, const UniversalPageId & rhs)
{
    return lhs == rhs.id;
}
} // namespace DB

template <>
struct fmt::formatter<DB::UniversalPageId>
{
    static constexpr auto parse(format_parse_context & ctx) -> decltype(ctx.begin())
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::UniversalPageId & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{}", Redact::keyToHexString(value.data(), value.size()));
    }
};