#include <Storages/MutableSupport.h>


namespace DB
{

const String MutableSupport::mmt_storage_name = "MutableMergeTree";
const String MutableSupport::txn_storage_name = "TxnMergeTree";
const String MutableSupport::delta_tree_storage_name = "DeltaMerge";

const String MutableSupport::tidb_pk_column_name = "_tidb_rowid";
const String MutableSupport::version_column_name = "_INTERNAL_VERSION";
const String MutableSupport::delmark_column_name = "_INTERNAL_DELMARK";

const DataTypePtr MutableSupport::tidb_pk_column_int_type = DataTypeFactory::instance().get("Int64");
const DataTypePtr MutableSupport::tidb_pk_column_string_type = DataTypeFactory::instance().get("String");
const DataTypePtr MutableSupport::version_column_type = DataTypeFactory::instance().get("UInt64");
const DataTypePtr MutableSupport::delmark_column_type = DataTypeFactory::instance().get("UInt8");

} // namespace DB
