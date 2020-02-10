#pragma once

#include <Storages/Transaction/DecodedRow.h>
#include <Storages/Transaction/TiKVKeyValue.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

namespace DB
{

class Context;
class StorageMergeTree;
class Region;
class TMTContext;

class IManageableStorage;
using ManageableStoragePtr = std::shared_ptr<IManageableStorage>;

using ColumnIds = google::dense_hash_set<ColumnID>;
using ColumnIdToIndex = google::dense_hash_map<ColumnID, size_t>;
constexpr ColumnID EmptyColumnID = TiDBPkColumnID - 1;
constexpr ColumnID DeleteColumnID = EmptyColumnID - 1;

// should keep the same way tidb does.
Field GenDefaultField(const TiDB::ColumnInfo & col_info);

struct RowPreDecoder
{
    const TiDB::TableInfo & table_info;
    const ColumnIdToIndex & column_lut;
    void preDecodeRow(const TiKVValue & value);
};

using CFDataPreDecodeQueue = std::deque<std::shared_ptr<const TiKVValue>>;
void tryPreDecodeTiKVValue(std::optional<CFDataPreDecodeQueue> && values, const ManageableStoragePtr & storage);
DecodedFields::const_iterator findByColumnID(const Int64 col_id, const DecodedFields & row);

} // namespace DB
