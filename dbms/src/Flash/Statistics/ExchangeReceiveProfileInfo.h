#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/types.h>

#include <vector>

namespace DB
{
struct ExchangeReceiveProfileInfo : public ConnectionProfileInfo
{
    Int64 partition_id;
    Int64 sender_task_id;

    explicit ExchangeReceiveProfileInfo(Int64 partition_id_, Int64 sender_task_id_)
        : ConnectionProfileInfo("ExchangeReceive")
        , partition_id(partition_id_)
        , sender_task_id(sender_task_id_)
    {}

    String toJson() const override;
};

using ExchangeReceiveProfileInfoPtr = std::shared_ptr<ExchangeReceiveProfileInfo>;
} // namespace DB