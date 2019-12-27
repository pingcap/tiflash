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

Field GenCustomField(const TiDB::ColumnInfo &);

std::optional<Field> GenFieldByColumnInfo(const TiDB::ColumnInfo & column);

struct ValueDecodeHelper
{
    const TiDB::TableInfo & table_info;
    const ColumnIdToIndex & schema_all_column_ids;
    bool forceDecodeTiKVValue(DecodedRow & decoded_row, DecodedRow & unknown);
    void forceDecodeTiKVValue(const TiKVValue & value);
};

using ExtraCFDataQueue = std::deque<std::shared_ptr<const TiKVValue>>;
void tryPreDecodeTiKVValue(std::optional<ExtraCFDataQueue> && values, StorageMergeTree & storage);

} // namespace DB
