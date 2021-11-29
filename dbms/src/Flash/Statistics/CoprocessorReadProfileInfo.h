#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/types.h>
#include <pingcap/coprocessor/Client.h>

#include <vector>

namespace DB
{
struct CoprocessorTaskInfo
{
    pingcap::kv::RegionVerID region_id;
    std::vector<pingcap::coprocessor::KeyRange> ranges;
    pingcap::kv::StoreType store_type;

    CoprocessorTaskInfo(const pingcap::coprocessor::copTask & cop_task)
        : region_id(cop_task.region_id)
        , ranges(cop_task.ranges)
        , store_type(cop_task.store_type)
    {}

    String toJson() const;
};

struct CoprocessorReadProfileInfo : public ConnectionProfileInfo
{
    std::vector<CoprocessorTaskInfo> cop_task_infos;

    explicit CoprocessorReadProfileInfo(const std::vector<CoprocessorTaskInfo> & cop_task_infos_)
        : ConnectionProfileInfo("CoprocessorRead")
    {
        cop_task_infos = cop_task_infos_;
    }

protected:
    String extraToJson() const override;
};

using CoprocessorReadProfileInfoPtr = std::shared_ptr<CoprocessorReadProfileInfo>;
} // namespace DB