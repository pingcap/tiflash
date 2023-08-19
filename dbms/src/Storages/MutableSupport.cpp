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

#include <Storages/MutableSupport.h>


namespace DB
{
const String MutableSupport::mmt_storage_name = "MutableMergeTree";
const String MutableSupport::txn_storage_name = "TxnMergeTree";
const String MutableSupport::delta_tree_storage_name = "DeltaMerge";

const String MutableSupport::tidb_pk_column_name = "_tidb_rowid";
const String MutableSupport::version_column_name = "_INTERNAL_VERSION";
const String MutableSupport::delmark_column_name = "_INTERNAL_DELMARK";
const String MutableSupport::extra_table_id_column_name = "_tidb_tid";

const DataTypePtr MutableSupport::tidb_pk_column_int_type = DataTypeFactory::instance().get("Int64");
const DataTypePtr MutableSupport::tidb_pk_column_string_type = DataTypeFactory::instance().get("String");
const DataTypePtr MutableSupport::version_column_type = DataTypeFactory::instance().get("UInt64");
const DataTypePtr MutableSupport::delmark_column_type = DataTypeFactory::instance().get("UInt8");
/// it should not be nullable, but TiDB does not set not null flag for extra_table_id_column_type, so has to align with TiDB
const DataTypePtr MutableSupport::extra_table_id_column_type = DataTypeFactory::instance().get("Nullable(Int64)");
;

} // namespace DB
