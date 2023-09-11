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

#include <Debug/MockComputeServerManager.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/MockExecutor/ExchangeReceiverBinder.h>
#include <Debug/MockExecutor/ExchangeSenderBinder.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Interpreters/Context.h>
#include <TiDB/Schema/TiDB.h>
#include <kvproto/mpp.pb.h>

namespace DB::mock
{
bool ExchangeReceiverBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeExchangeReceiver);
    tipb_executor->set_executor_id(name);
    tipb_executor->set_fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count);
    tipb::ExchangeReceiver * exchange_receiver = tipb_executor->mutable_exchange_receiver();

    if (exchange_sender)
        exchange_receiver->set_tp(exchange_sender->getType());

    for (auto & field : output_schema)
    {
        auto tipb_type = TiDB::columnInfoToFieldType(field.second);
        tipb_type.set_collate(collator_id);

        auto * field_type = exchange_receiver->add_field_types();
        *field_type = tipb_type;
    }

    auto it = mpp_info.receiver_source_task_ids_map.find(name);
    if (it == mpp_info.receiver_source_task_ids_map.end())
        throw Exception("Can not found mpp receiver info");

    auto size = it->second.size();
    for (size_t i = 0; i < size; ++i)
    {
        mpp::TaskMeta meta;
        fillTaskMetaWithMPPInfo(meta, mpp_info);
        meta.set_task_id(it->second[i]);
        meta.set_partition_id(i);
        auto addr = context.isMPPTest() ? tests::MockComputeServerManager::instance().getServerConfigMap()[i].addr
                                        : Debug::LOCAL_HOST;
        meta.set_address(addr);
        auto * meta_string = exchange_receiver->add_encoded_task_meta();
        meta.AppendToString(meta_string);
    }
    return true;
}


void ExchangeReceiverBinder::toMPPSubPlan(
    size_t & executor_index,
    const DAGProperties & properties,
    std::unordered_map<
        String,
        std::pair<std::shared_ptr<ExchangeReceiverBinder>, std::shared_ptr<ExchangeSenderBinder>>> & exchange_map)
{
    RUNTIME_CHECK_MSG(exchange_sender, "exchange_sender must not be nullptr in toMPPSubPlan");
    exchange_sender->toMPPSubPlan(executor_index, properties, exchange_map);
    exchange_map[name] = std::make_pair(shared_from_this(), exchange_sender);
}

ExecutorBinderPtr compileExchangeReceiver(
    size_t & executor_index,
    DAGSchema schema,
    uint64_t fine_grained_shuffle_stream_count,
    const std::shared_ptr<ExchangeSenderBinder> & exchange_sender)
{
    ExecutorBinderPtr exchange_receiver = std::make_shared<mock::ExchangeReceiverBinder>(
        executor_index,
        schema,
        fine_grained_shuffle_stream_count,
        exchange_sender);
    return exchange_receiver;
}
} // namespace DB::mock
