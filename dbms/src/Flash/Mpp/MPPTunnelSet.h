#pragma once

#include <Flash/Mpp/MPPTunnel.h>
#include <tipb/select.pb.h>

#include <boost/noncopyable.hpp>

namespace DB
{
template <typename Tunnel>
class MPPTunnelSetBase : private boost::noncopyable
{
public:
    using TunnelPtr = std::shared_ptr<Tunnel>;

    void clearExecutionSummaries(tipb::SelectResponse & response);

    /// for both broadcast writing and partition writing, only
    /// return meaningful execution summary for the first tunnel,
    /// because in TiDB, it does not know enough information
    /// about the execution details for the mpp query, it just
    /// add up all the execution summaries for the same executor,
    /// so if return execution summary for all the tunnels, the
    /// information in TiDB will be amplified, which may make
    /// user confused.
    // this is a broadcast writing.
    void write(tipb::SelectResponse & response);

    // this is a partition writing.
    void write(tipb::SelectResponse & response, int16_t partition_id);

    uint16_t getPartitionNum() const { return tunnels.size(); }

    void addTunnel(const TunnelPtr & tunnel) { tunnels.push_back(tunnel); }

private:
    std::vector<TunnelPtr> tunnels;
};

class MPPTunnelSet : public MPPTunnelSetBase<MPPTunnel>
{
public:
    using Base = MPPTunnelSetBase<MPPTunnel>;
    using Base::Base;
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

} // namespace DB
