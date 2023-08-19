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

#include <Debug/MockExecutor/ExchangeReceiverBinder.h>
#include <Debug/MockExecutor/ExecutorBinder.h>

namespace DB::mock
{
bool ExchangeReceiverBinder::toTiPBExecutor(tipb::Executor * tipb_executor, int32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeExchangeReceiver);
    tipb_executor->set_executor_id(name);
    tipb_executor->set_fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count);
    tipb::ExchangeReceiver * exchange_receiver = tipb_executor->mutable_exchange_receiver();

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
        meta.set_start_ts(mpp_info.start_ts);
        meta.set_task_id(it->second[i]);
        meta.set_partition_id(i);
        auto addr = context.isMPPTest() ? tests::MockComputeServerManager::instance().getServerConfigMap()[i].addr : Debug::LOCAL_HOST;
        meta.set_address(addr);
        auto * meta_string = exchange_receiver->add_encoded_task_meta();
        meta.AppendToString(meta_string);
    }
    return true;
}


ExecutorBinderPtr compileExchangeReceiver(size_t & executor_index, DAGSchema schema, uint64_t fine_grained_shuffle_stream_count)
{
    ExecutorBinderPtr exchange_receiver = std::make_shared<mock::ExchangeReceiverBinder>(executor_index, schema, fine_grained_shuffle_stream_count);
    return exchange_receiver;
}
} // namespace DB::mock
