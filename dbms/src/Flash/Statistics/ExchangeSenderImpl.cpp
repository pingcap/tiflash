// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExchangeSenderImpl.h>

namespace DB
{
String MPPTunnelDetail::toJson() const
{
    return fmt::format(
        R"({{"tunnel_id":"{}","sender_target_task_id":{},"sender_target_host":"{}","is_local":{},"conn_type":{},"packets":{},"bytes":{}}})",
        tunnel_id,
        sender_target_task_id,
        sender_target_host,
        is_local,
        conn_profile_info.getTypeString(),
        conn_profile_info.packets,
        conn_profile_info.bytes);
}

void ExchangeSenderStatistics::appendExtraJson(FmtBuffer & fmt_buffer) const
{
    fmt_buffer.fmtAppend(
        R"("partition_num":{},"sender_target_task_ids":[{}],"exchange_type":"{}","connection_details":[)",
        partition_num,
        fmt::join(sender_target_task_ids, ","),
        getExchangeTypeName(exchange_type));
    fmt_buffer.joinStr(
        mpp_tunnel_details.cbegin(),
        mpp_tunnel_details.cend(),
        [](const auto & p, FmtBuffer & bf) { bf.append(p.toJson()); },
        ",");
    fmt_buffer.append("]");
}

void ExchangeSenderStatistics::collectExtraRuntimeDetail()
{
    const auto & mpp_tunnels = dag_context.tunnel_set->getTunnels();
    for (UInt16 i = 0; i < partition_num; ++i)
    {
        const auto & connection_profile_info = mpp_tunnels[i]->getConnectionProfileInfo();
        mpp_tunnel_details[i].conn_profile_info.packets = connection_profile_info.packets;
        mpp_tunnel_details[i].conn_profile_info.bytes += connection_profile_info.bytes;
        base.updateSendConnectionInfo(connection_profile_info);
    }
}

ExchangeSenderStatistics::ExchangeSenderStatistics(const tipb::Executor * executor, DAGContext & dag_context_)
    : ExchangeSenderStatisticsBase(executor, dag_context_)
{
    RUNTIME_CHECK(dag_context.isMPPTask());

    assert(executor->tp() == tipb::ExecType::TypeExchangeSender);
    const auto & exchange_sender_executor = executor->exchange_sender();
    RUNTIME_CHECK(exchange_sender_executor.has_tp());
    exchange_type = exchange_sender_executor.tp();
    partition_num = exchange_sender_executor.encoded_task_meta_size();

    const auto & mpp_tunnel_set = dag_context.tunnel_set;
    RUNTIME_CHECK(partition_num == mpp_tunnel_set->getPartitionNum());
    const auto & mpp_tunnels = mpp_tunnel_set->getTunnels();

    for (int i = 0; i < exchange_sender_executor.encoded_task_meta_size(); ++i)
    {
        mpp::TaskMeta task_meta;
        if (unlikely(!task_meta.ParseFromString(exchange_sender_executor.encoded_task_meta(i))))
            throw TiFlashException(
                "Failed to decode task meta info in ExchangeSender",
                Errors::Coprocessor::BadRequest);
        sender_target_task_ids.push_back(task_meta.task_id());

        const auto & mpp_tunnel = mpp_tunnels[i];
        mpp_tunnel_details.emplace_back(
            mpp_tunnel->getConnectionProfileInfo(),
            mpp_tunnel->id(),
            task_meta.task_id(),
            task_meta.address(),
            mpp_tunnel->isLocal());
    }

    // for root task, exchange_sender_executor.task_meta[0].address is blank or not tidb host
    // TODO pass tidb host in exchange_sender_executor.task_meta[0]
    if (dag_context.isRootMPPTask())
    {
        RUNTIME_CHECK(mpp_tunnel_details.size() == 1);
        mpp_tunnel_details.back().sender_target_host = dag_context.tidb_host;
    }
}
} // namespace DB
