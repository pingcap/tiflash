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
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <TiDB/Decode/TypeMapping.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB::ExchangeSenderInterpreterHelper
{
std::vector<Int64> genPartitionColIds(const ::google::protobuf::RepeatedPtrField<::tipb::Expr> & partition_keys)
{
    std::vector<Int64> partition_col_ids;
    for (const auto & part_key : partition_keys)
    {
        if (unlikely(!isColumnExpr(part_key)))
        {
            throw TiFlashException(
                fmt::format(
                    "{}: Invalid plan, in ExchangeSender, part_key of ExchangeSender must be column",
                    __PRETTY_FUNCTION__),
                Errors::Coprocessor::BadRequest);
        }
        partition_col_ids.emplace_back(decodeDAGInt64(part_key.val()));
    }
    return partition_col_ids;
}

TiDB::TiDBCollators genPartitionColCollators(
    const ::google::protobuf::RepeatedPtrField<::tipb::Expr> & partition_keys,
    const ::google::protobuf::RepeatedPtrField<::tipb::FieldType> & types)
{
    TiDB::TiDBCollators partition_col_collators;
    auto type_num = types.size();
    /// in case TiDB is an old version, it has no collation info
    bool has_collator_info = type_num != 0;
    if (unlikely(has_collator_info && partition_keys.size() != type_num))
    {
        throw TiFlashException(
            fmt::format(
                "{}: Invalid plan, in ExchangeSender, the length of partition_keys and types is not the same when TiDB "
                "new collation is enabled",
                __PRETTY_FUNCTION__),
            Errors::Coprocessor::BadRequest);
    }
    for (int i = 0; i < partition_keys.size(); ++i)
    {
        const auto & expr = partition_keys[i];
        if (has_collator_info && removeNullable(getDataTypeByFieldTypeForComputingLayer(expr.field_type()))->isString())
        {
            partition_col_collators.emplace_back(getCollatorFromFieldType(types.at(i)));
        }
        else
        {
            partition_col_collators.emplace_back(nullptr);
        }
    }
    return partition_col_collators;
}
} // namespace DB::ExchangeSenderInterpreterHelper
