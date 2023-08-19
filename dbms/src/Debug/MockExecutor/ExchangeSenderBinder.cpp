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

#include <Debug/MockExecutor/ExchangeSenderBinder.h>
#include <Debug/MockExecutor/ExecutorBinder.h>

namespace DB::mock
{
bool ExchangeSenderBinder::toTiPBExecutor(tipb::Executor * tipb_executor, int32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeExchangeSender);
    tipb_executor->set_executor_id(name);
    tipb::ExchangeSender * exchange_sender = tipb_executor->mutable_exchange_sender();
    exchange_sender->set_tp(type);
    for (auto i : partition_keys)
    {
        auto * expr = exchange_sender->add_partition_keys();
        expr->set_tp(tipb::ColumnRef);
        WriteBufferFromOwnString ss;
        encodeDAGInt64(i, ss);
        expr->set_val(ss.releaseStr());
        auto tipb_type = TiDB::columnInfoToFieldType(output_schema[i].second);
        *expr->mutable_field_type() = tipb_type;
        tipb_type.set_collate(collator_id);
        *exchange_sender->add_types() = tipb_type;
    }

    int i = 0;
    for (auto task_id : mpp_info.sender_target_task_ids)
    {
        mpp::TaskMeta meta;
        meta.set_start_ts(mpp_info.start_ts);
        meta.set_task_id(task_id);
        meta.set_partition_id(i);
        auto addr = context.isMPPTest() ? tests::MockComputeServerManager::instance().getServerConfigMap()[i++].addr : Debug::LOCAL_HOST;
        meta.set_address(addr);

        auto * meta_string = exchange_sender->add_encoded_task_meta();
        meta.AppendToString(meta_string);
    }

    for (auto & field : output_schema)
    {
        auto tipb_type = TiDB::columnInfoToFieldType(field.second);
        tipb_type.set_collate(collator_id);
        auto * field_type = exchange_sender->add_all_field_types();
        *field_type = tipb_type;
    }

    auto * child_executor = exchange_sender->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}

tipb::ExchangeType ExchangeSenderBinder::getType() const
{
    return type;
}

ExecutorBinderPtr compileExchangeSender(ExecutorBinderPtr input, size_t & executor_index, tipb::ExchangeType exchange_type)
{
    ExecutorBinderPtr exchange_sender = std::make_shared<mock::ExchangeSenderBinder>(executor_index, input->output_schema, exchange_type);
    exchange_sender->children.push_back(input);
    return exchange_sender;
}
} // namespace DB::mock
