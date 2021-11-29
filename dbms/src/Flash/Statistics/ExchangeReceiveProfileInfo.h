#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/types.h>

#include <vector>

namespace DB
{
struct ExchangeReceiveProfileInfo : public ConnectionProfileInfo
{
    Int64 partition_id;
    Int64 receiver_source_task_id;

    explicit ExchangeReceiveProfileInfo(Int64 partition_id_, Int64 receiver_source_task_id_)
        : ConnectionProfileInfo("ExchangeReceive")
        , partition_id(partition_id_)
        , receiver_source_task_id(receiver_source_task_id_)
    {}

protected:
    String extraToJson() const override;
};

using ExchangeReceiveProfileInfoPtr = std::shared_ptr<ExchangeReceiveProfileInfo>;
} // namespace DB