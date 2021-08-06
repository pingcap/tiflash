#include <Common/Exception.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{

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
    for (size_t i = 0; i < tunnels.size(); ++i)
        write(response, i);
}

void MPPTunnelSet::write(tipb::SelectResponse & response, int16_t partition_id)
{
    if (partition_id != 0)
        clearExecutionSummaries(response);

    mpp::MPPDataPacket packet;
    if (!response.SerializeToString(packet.mutable_data()))
        throw Exception("Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));
    tunnels[partition_id]->write(packet);
}

} // namespace DB

