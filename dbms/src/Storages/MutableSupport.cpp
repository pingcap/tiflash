#include <Storages/MutableSupport.h>


namespace DB
{

const std::string MutableSupport::storage_name = "MutableMergeTree";
const std::string MutableSupport::txn_storage_name = "TxnMergeTree";
const std::string MutableSupport::tidb_pk_column_name = "_tidb_rowid";
const std::string MutableSupport::version_column_name = "_INTERNAL_VERSION";
const std::string MutableSupport::delmark_column_name = "_INTERNAL_DELMARK";
}
