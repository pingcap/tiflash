#include <Common/FmtUtils.h>
#include <Storages/Page/V3/Blob/GCInfo.h>

#include <magic_enum.hpp>


template <>
struct fmt::formatter<DB::PS::V3::BlobFileGCInfo>
{
    static constexpr auto parse(format_parse_context & ctx) -> decltype(ctx.begin())
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PS::V3::BlobFileGCInfo & i, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "<id:{} rate:{:.2f}>", i.blob_id, i.valid_rate);
    }
};
template <>
struct fmt::formatter<DB::PS::V3::BlobFileTruncateInfo>
{
    static constexpr auto parse(format_parse_context & ctx) -> decltype(ctx.begin())
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PS::V3::BlobFileTruncateInfo & i, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "<id:{} origin:{} truncate:{} rate:{:.2f}>", i.blob_id, i.origin_size, i.truncated_size, i.valid_rate);
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
    return fmt::format("{} {} {} {}",
                       toTypeString(ReadOnly),
                       toTypeString(Unchanged),
                       toTypeString(FullGC),
                       toTypeTruncateString(Truncated));
}

String BlobStoreGCInfo::toTypeString(const Type type_index) const
{
    if (blob_gc_info[type_index].empty())
        return fmt::format("{{{}: [null]}}", magic_enum::enum_name(type_index));

    // e.g. {FullGC: [<id:4 rate:0.16>]}}
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("{{{}: [", magic_enum::enum_name(type_index))
        .joinStr(
            blob_gc_info[type_index].begin(),
            blob_gc_info[type_index].end(),
            [](const auto & i, FmtBuffer & fb) {
                fb.fmtAppend("{}", i);
            },
            ", ")
        .append("]}}");
    return fmt_buf.toString();
}

String BlobStoreGCInfo::toTypeTruncateString(const Type type_index) const
{
    if (blob_gc_truncate_info.empty())
        return fmt::format("{{{}: [null]}}", magic_enum::enum_name(type_index));

    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("{{{}: [", type_index)
        .joinStr(
            blob_gc_truncate_info.begin(),
            blob_gc_truncate_info.end(),
            [](const auto & i, FmtBuffer & fb) {
                fb.fmtAppend("{}", i);
            },
            ", ")
        .append("]}}");
    return fmt_buf.toString();
}
} // namespace DB::PS::V3
