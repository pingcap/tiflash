#include <Common/TiFlashException.h>
#include <Common/joinStr.h>
#include <Flash/Statistics/CoprocessorReadProfileInfo.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>

namespace DB
{
namespace
{
inline String storeTypeToString(const pingcap::kv::StoreType & store_type)
{
    switch (store_type)
    {
    case pingcap::kv::StoreType::TiKV:
        return "TiKV";
    case pingcap::kv::StoreType::TiFlash:
        return "TiFlash";
    default:
        throw TiFlashException("unknown store type", Errors::Coprocessor::BadRequest);
    }
}
} // namespace

String CoprocessorTaskInfo::toJson() const
{
    FmtBuffer buffer;
    buffer.fmtAppend(R"({"region_id":"{}","ranges":[)", region_id.toString());
    joinStr(
        ranges.cbegin(),
        ranges.cend(),
        buffer,
        [](const pingcap::coprocessor::KeyRange & r, FmtBuffer & fb) { fb.fmtAppend("\"[{},{})\"", r.start_key, r.end_key); },
        ",");
    buffer.fmtAppend(R"(],"store_type":"{}"}})", storeTypeToString(store_type));
    return buffer.toString();
}

String CoprocessorReadProfileInfo::extraToJson() const
{
    return fmt::format(
        R"(,"cop_task_infos":{})",
        arrayToJson<false>(cop_task_infos));
}
} // namespace DB