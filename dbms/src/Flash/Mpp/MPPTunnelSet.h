#pragma once

#include <boost/noncopyable.hpp>
#include <tipb/select.pb.h>

#include <Flash/Mpp/MPPTunnel.h>

namespace DB
{
class MPPTunnelSet : private boost::noncopyable
{
public:
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

    void addTunnel(const MPPTunnelPtr & tunnel) { tunnels.push_back(tunnel); }
private:
    std::vector<MPPTunnelPtr> tunnels;
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

} // namespace DB

