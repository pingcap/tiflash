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

#include <Common/FmtUtils.h>
#include <Storages/Page/V3/Blob/GCInfo.h>

#include <magic_enum.hpp>


template <>
struct fmt::formatter<DB::PS::V3::BlobFileGCInfo>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PS::V3::BlobFileGCInfo & i, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "<blob_id={} rate={:.2f}>", i.blob_id, i.valid_rate);
    }
};
template <>
struct fmt::formatter<DB::PS::V3::BlobFileTruncateInfo>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PS::V3::BlobFileTruncateInfo & i, FormatContext & ctx) const
    {
        return fmt::format_to(
            ctx.out(),
            "<blob_id={} origin={} truncate={} rate={:.2f}>",
            i.blob_id,
            i.origin_size,
            i.truncated_size,
            i.valid_rate);
    }
};

namespace DB::PS::V3
{

Poco::Message::Priority BlobStoreGCInfo::getLoggingLevel() const
{
    if (blob_gc_info[FullGC].empty() && blob_gc_truncate_info.empty())
        return Poco::Message::PRIO_DEBUG;
    return Poco::Message::PRIO_INFORMATION;
}

String BlobStoreGCInfo::toString() const
{
    return fmt::format(
        "{} {} {} {}",
        toTypeString(ReadOnly),
        toTypeString(Unchanged),
        toTypeString(FullGC),
        toTypeTruncateString(Truncated));
}

String BlobStoreGCInfo::toTypeString(const Type type_index) const
{
    if (blob_gc_info[type_index].empty())
        return fmt::format("{{{}: [null]}}", magic_enum::enum_name(type_index));

    // e.g. {FullGC: [<blob_id=4 rate=0.16>]}
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("{{{}: [", magic_enum::enum_name(type_index))
        .joinStr(
            blob_gc_info[type_index].begin(),
            blob_gc_info[type_index].end(),
            [](const auto & i, FmtBuffer & fb) { fb.fmtAppend("{}", i); },
            ", ")
        .append("]}");
    return fmt_buf.toString();
}

String BlobStoreGCInfo::toTypeTruncateString(const Type type_index) const
{
    if (blob_gc_truncate_info.empty())
        return fmt::format("{{{}: [null]}}", magic_enum::enum_name(type_index));

    // e.g. {Truncated: [<blob_id=221 origin=0 truncate=0 rate=0.00>]}
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("{{{}: [", magic_enum::enum_name(type_index))
        .joinStr(
            blob_gc_truncate_info.begin(),
            blob_gc_truncate_info.end(),
            [](const auto & i, FmtBuffer & fb) { fb.fmtAppend("{}", i); },
            ", ")
        .append("]}");
    return fmt_buf.toString();
}
} // namespace DB::PS::V3
