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

#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/TableScanBinder.h>
#include <Storages/MutableSupport.h>

#include <cstdlib>

namespace DB::mock
{
bool TableScanBinder::toTiPBExecutor(tipb::Executor * tipb_executor, int32_t, const MPPInfo &, const Context &)
{
    if (table_info.is_partition_table)
        buildPartionTable(tipb_executor);
    else
        buildTable(tipb_executor);

    return true;
}

void TableScanBinder::columnPrune(std::unordered_set<String> & used_columns)
{
    DAGSchema new_schema;
    for (const auto & col : output_schema)
    {
        for (const auto & used_col : used_columns)
        {
            if (splitQualifiedName(used_col).column_name == splitQualifiedName(col.first).column_name && splitQualifiedName(used_col).table_name == splitQualifiedName(col.first).table_name)
            {
                new_schema.push_back({used_col, col.second});
            }
        }
    }

    output_schema = new_schema;
}

TableID TableScanBinder::getTableId() const
{
    return table_info.id;
}

void TableScanBinder::setTipbColumnInfo(tipb::ColumnInfo * ci, const DAGColumnInfo & dag_column_info) const
{
    auto names = splitQualifiedName(dag_column_info.first);
    Int64 column_id = 0;
    if (names.column_name == MutableSupport::tidb_pk_column_name)
        column_id = -1;
    else if (dag_column_info.second.hasGeneratedColumnFlag())
        column_id = dag_column_info.second.id;
    else
        column_id = table_info.getColumnID(names.column_name);
    setTipbColumnInfo(ci, dag_column_info, column_id);
}

void TableScanBinder::setTipbColumnInfo(tipb::ColumnInfo * ci, const DAGColumnInfo & dag_column_info, Int64 column_id)
{
    auto names = splitQualifiedName(dag_column_info.first);
    if (names.column_name == MutableSupport::tidb_pk_column_name)
        ci->set_column_id(-1);
    else
        ci->set_column_id(column_id);
    ci->set_tp(dag_column_info.second.tp);
    ci->set_flag(dag_column_info.second.flag);
    ci->set_columnlen(dag_column_info.second.flen);
    ci->set_decimal(dag_column_info.second.decimal);
    if (!dag_column_info.second.elems.empty())
    {
        for (const auto & pair : dag_column_info.second.elems)
        {
            ci->add_elems(pair.first);
        }
    }
}

void TableScanBinder::buildPartionTable(tipb::Executor * tipb_executor)
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

void TableScanBinder::buildTable(tipb::Executor * tipb_executor)
{
    tipb_executor->set_tp(tipb::ExecType::TypeTableScan);
    tipb_executor->set_executor_id(name);
    auto * ts = tipb_executor->mutable_tbl_scan();
    ts->set_table_id(table_info.id);
    for (const auto & info : output_schema)
        setTipbColumnInfo(ts->add_columns(), info);
}

ExecutorBinderPtr compileTableScan(size_t & executor_index, TableInfo & table_info, const String & db, const String & table_name, bool append_pk_column)
{
    DAGSchema ts_output;
    for (const auto & column_info : table_info.columns)
    {
        ColumnInfo ci;
        ci.id = column_info.id;
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
        ci.id = TiDBPkColumnID;
        ci.setPriKeyFlag();
        ci.setNotNullFlag();
        ts_output.emplace_back(std::make_pair(MutableSupport::tidb_pk_column_name, std::move(ci)));
    }
    // if (random_gennerated_column)
    int gen_col_num = 100;
    auto ori_ts_output = ts_output;
    size_t ori_output_index = 0;
    for (size_t i = 0; i < ori_ts_output.size() + gen_col_num; ++i)
    {
        if (std::rand() % 2 == 0 || ori_output_index >= ori_ts_output.size())
        {
            TiDB::ColumnInfo ci;
            ci.tp = TiDB::TP::TypeLong;
            ci.flag = TiDB::ColumnFlag::ColumnFlagGeneratedColumn;
            String gen_column_name = fmt::format("gen_col_{}", i);
            ci.name = gen_column_name;
            ts_output.emplace_back(std::make_pair(db + "." + table_name + "." + gen_column_name, std::move(ci)));
        }
        else
        {
            ts_output.emplace_back(ori_ts_output[ori_output_index]);
        }
    }

    return std::make_shared<mock::TableScanBinder>(executor_index, ts_output, table_info);
}
} // namespace DB::mock
