#pragma once

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/types.h>

namespace DB
{
struct MPPTunnelProfileInfo : public ConnectionProfileInfo
{
    String tunnel_id;

    explicit MPPTunnelProfileInfo(const String & tunnel_id_)
        : ConnectionProfileInfo("MPPTunnel")
        , tunnel_id(tunnel_id_)
    {}

    String toJson() const override;
};

using MPPTunnelProfileInfoPtr = std::shared_ptr<MPPTunnelProfileInfo>;
} // namespace DB