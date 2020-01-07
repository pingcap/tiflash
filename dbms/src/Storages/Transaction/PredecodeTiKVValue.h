#pragma once

#include <Storages/Transaction/TiKVDecodedValue.h>
#include <Storages/Transaction/TiKVKeyValue.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

namespace DB
{

class Context;
class StorageMergeTree;
class Region;
class TMTContext;

using ColumnIds = google::dense_hash_set<ColumnID>;
using ColumnIdToIndex = google::dense_hash_map<ColumnID, size_t>;
constexpr ColumnID EmptyColumnID = InvalidColumnID - 1;
constexpr ColumnID DeleteColumnID = EmptyColumnID - 1;

// should keep the same way tidb does.
Field GenDefaultField(const TiDB::ColumnInfo & col_info);

struct ValueDecodeHelper
{
    const TiDB::TableInfo & table_info;
    const ColumnIdToIndex & schema_all_column_ids;
    void forceDecodeTiKVValue(const TiKVValue & value);

private:
    bool forceDecodeTiKVValue(DecodedFields & decoded_row, DecodedFields & unknown);
};

using ExtraCFDataQueue = std::deque<std::shared_ptr<const TiKVValue>>;
void tryPreDecodeTiKVValue(std::optional<ExtraCFDataQueue> && values, StorageMergeTree & storage);
DecodedFields::const_iterator findByColumnID(const Int64 col_id, const DecodedFields & row);

} // namespace DB
