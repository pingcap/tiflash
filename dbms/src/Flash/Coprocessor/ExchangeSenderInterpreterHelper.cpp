// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <Storages/Transaction/TypeMapping.h>
#include <common/logger_useful.h>

namespace DB::ExchangeSenderInterpreterHelper
{
std::pair<std::vector<Int64>, TiDB::TiDBCollators> genPartitionColIdsAndCollators(
    const tipb::ExchangeSender & exchange_sender,
    const LoggerPtr & log)
{
    /// get partition column ids
    const auto & part_keys = exchange_sender.partition_keys();
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators partition_col_collators;
    /// in case TiDB is an old version, it has no collation info
    bool has_collator_info = exchange_sender.types_size() != 0;
    if (has_collator_info && part_keys.size() != exchange_sender.types_size())
    {
        throw TiFlashException(
            fmt::format("{}: Invalid plan, in ExchangeSender, the length of partition_keys and types is not the same when TiDB new collation is enabled", __PRETTY_FUNCTION__),
            Errors::Coprocessor::BadRequest);
    }
    for (int i = 0; i < part_keys.size(); ++i)
    {
        const auto & expr = part_keys[i];
        RUNTIME_ASSERT(isColumnExpr(expr), log, "part_key of ExchangeSender must be column");
        auto column_index = decodeDAGInt64(expr.val());
        partition_col_ids.emplace_back(column_index);
        if (has_collator_info && removeNullable(getDataTypeByFieldTypeForComputingLayer(expr.field_type()))->isString())
        {
            partition_col_collators.emplace_back(getCollatorFromFieldType(exchange_sender.types(i)));
        }
        else
        {
            partition_col_collators.emplace_back(nullptr);
        }
    }
    return {partition_col_ids, partition_col_collators};
}
} // namespace DB::ExchangeSenderInterpreterHelper
