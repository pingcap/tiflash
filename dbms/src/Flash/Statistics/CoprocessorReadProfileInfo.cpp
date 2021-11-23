#include <Common/TiFlashException.h>
#include <Common/joinStr.h>
#include <Flash/Statistics/CoprocessorReadProfileInfo.h>
#include <Flash/Statistics/ExecutorStatisticsUtils.h>

namespace DB
{
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
    buffer.append(R"(],"store_type":")");
    switch (store_type)
    {
    case pingcap::kv::StoreType::TiKV:
        buffer.append(R"(TiKV"})");
        break;
        case pingcap::kv::StoreType::TiFlash:
            buffer.append(R"(TiFlash"})");
            break;
            default:
                throw TiFlashException("unknown store type", Errors::Coprocessor::BadRequest);
    }
    return buffer.toString();
}

String CoprocessorReadProfileInfo::toJson() const
{
    return fmt::format(
        R"({{"connection_type":"{}","rows":{},"blocks":{},"bytes":{},"cop_task_infos":{}}})",
        connection_type,
        rows,
        blocks,
        bytes,
        arrayToJson<false>(cop_task_infos));
}
} // namespace DB