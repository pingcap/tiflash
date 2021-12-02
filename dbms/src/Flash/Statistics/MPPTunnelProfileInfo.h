#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/types.h>

namespace DB
{
struct MPPTunnelProfileInfo : public ConnectionProfileInfo
{
    String tunnel_id;

    Int64 sender_target_task_id;

    explicit MPPTunnelProfileInfo(const String & tunnel_id_, Int64 sender_target_task_id_)
        : ConnectionProfileInfo("MPPTunnel")
        , tunnel_id(tunnel_id_)
        , sender_target_task_id(sender_target_task_id_)
    {}

protected:
    String extraToJson() const override;
};

using MPPTunnelProfileInfoPtr = std::shared_ptr<MPPTunnelProfileInfo>;
} // namespace DB