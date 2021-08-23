#include <Common/Exception.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <fmt/core.h>

namespace DB
{

static mpp::MPPDataPacket serializeToPacket(const tipb::SelectResponse & response)
{
    mpp::MPPDataPacket packet;
    if (!response.SerializeToString(packet.mutable_data()))
        throw Exception(fmt::format("Fail to serialize response, response size: {}", response.ByteSizeLong()));
    return packet;
}

void MPPTunnelSet::clearExecutionSummaries(tipb::SelectResponse & response)
{
    /// can not use response.clear_execution_summaries() because
    /// TiDB assume all the executor should return execution summary
    for (int i = 0; i < response.execution_summaries_size(); i++)
    {
        auto * mutable_execution_summary = response.mutable_execution_summaries(i);
        mutable_execution_summary->set_num_produced_rows(0);
        mutable_execution_summary->set_num_iterations(0);
        mutable_execution_summary->set_concurrency(0);
    }
}

void MPPTunnelSet::write(tipb::SelectResponse & response)
{
    tunnels[0]->write(serializeToPacket(response));

    if (tunnels.size() > 1)
    {
        clearExecutionSummaries(response);
        auto packet = serializeToPacket(response);
        for (size_t i = 1; i < tunnels.size(); ++i)
            tunnels[i]->write(packet);
    }
}

void MPPTunnelSet::write(tipb::SelectResponse & response, int16_t partition_id)
{
    if (partition_id != 0)
        clearExecutionSummaries(response);

    tunnels[partition_id]->write(serializeToPacket(response));
}

} // namespace DB

