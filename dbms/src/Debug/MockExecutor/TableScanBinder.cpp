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

#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/TableScanBinder.h>
namespace DB::mock
{

bool TableScanBinder::toTiPBExecutor(tipb::Executor * tipb_executor, int32_t, const MPPInfo &, const Context &)
{
    if (table_info.is_partition_table)
    {
        tipb_executor->set_tp(tipb::ExecType::TypePartitionTableScan);
        tipb_executor->set_executor_id(name);
        auto * partition_ts = tipb_executor->mutable_partition_table_scan();
        partition_ts->set_table_id(table_info.id);
        for (const auto & info : output_schema)
            setTipbColumnInfo(partition_ts->add_columns(), info);
        for (const auto & partition : table_info.partition.definitions)
            partition_ts->add_partition_ids(partition.id);
    }
    else
    {
        tipb_executor->set_tp(tipb::ExecType::TypeTableScan);
        tipb_executor->set_executor_id(name);
        auto * ts = tipb_executor->mutable_tbl_scan();
        ts->set_table_id(table_info.id);
        for (const auto & info : output_schema)
            setTipbColumnInfo(ts->add_columns(), info);
    }
    return true;
}

ExecutorBinderPtr compileTableScan(size_t & executor_index, TableInfo & table_info, const String & db, const String & table_name, bool append_pk_column)
{
    DAGSchema ts_output;
    for (const auto & column_info : table_info.columns)
    {
        ColumnInfo ci;
        ci.tp = column_info.tp;
        ci.flag = column_info.flag;
        ci.flen = column_info.flen;
        ci.decimal = column_info.decimal;
        ci.elems = column_info.elems;
        ci.default_value = column_info.default_value;
        ci.origin_default_value = column_info.origin_default_value;
        /// use qualified name as the column name to handle multiple table queries, not very
        /// efficient but functionally enough for mock test
        ts_output.emplace_back(std::make_pair(db + "." + table_name + "." + column_info.name, std::move(ci)));
    }
    if (append_pk_column)
    {
        ColumnInfo ci;
        ci.tp = TiDB::TypeLongLong;
        ci.setPriKeyFlag();
        ci.setNotNullFlag();
        ts_output.emplace_back(std::make_pair(MutableSupport::tidb_pk_column_name, std::move(ci)));
    }

    return std::make_shared<mock::TableScanBinder>(executor_index, ts_output, table_info);
}
} // namespace DB::mock