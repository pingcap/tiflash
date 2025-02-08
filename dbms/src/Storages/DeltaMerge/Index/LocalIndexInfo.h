// Copyright 2024 PingCAP, Inc.
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

#pragma once

#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/VectorIndex.h>

#include <span>

namespace TiDB
{
struct TableInfo;
struct ColumnInfo;
struct IndexInfo;
} // namespace TiDB

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB
namespace DB::DM
{
enum class IndexType
{
    Vector = 1,
};

struct LocalIndexInfo
{
    IndexType type;
    // If the index is defined on TiDB::ColumnInfo, use EmptyIndexID as index_id
    IndexID index_id = DB::EmptyIndexID;
    // Which column_id the index is built on
    ColumnID column_id = DB::EmptyColumnID;
    // Now we only support vector index.
    // In the future, we may support more types of indexes, using std::variant.
    TiDB::VectorIndexDefinitionPtr index_definition;
};

using LocalIndexInfos = std::vector<LocalIndexInfo>;
using LocalIndexInfosPtr = std::shared_ptr<LocalIndexInfos>;
using LocalIndexInfosSnapshot = std::shared_ptr<const LocalIndexInfos>;

LocalIndexInfosPtr initLocalIndexInfos(const TiDB::TableInfo & table_info, const LoggerPtr & logger);

class LocalIndexInfosChangeset
{
public:
    LocalIndexInfosPtr new_local_index_infos;
    // This vector store all the keep/added/dropped index IDs.
    // The keep indexes: [0, added_indexes_offset)
    // The added indexes: [added_indexes_offset, dropped_indexes_offset)
    // The dropped indexes: [dropped_index_offset, end-of-the-vector)
    const std::vector<IndexID> all_indexes;
    const size_t added_indexes_offset;
    const size_t dropped_indexes_offset;

public:
    std::span<const IndexID> keepIndexes() const
    {
        assert(added_indexes_offset <= all_indexes.size());
        return std::span(all_indexes.begin(), all_indexes.begin() + added_indexes_offset);
    }
    std::span<const IndexID> addedIndexes() const
    {
        assert(added_indexes_offset <= dropped_indexes_offset && added_indexes_offset <= all_indexes.size());
        assert(dropped_indexes_offset <= all_indexes.size());
        return std::span(all_indexes.begin() + added_indexes_offset, all_indexes.begin() + dropped_indexes_offset);
    }
    std::span<const IndexID> droppedIndexes() const
    {
        assert(dropped_indexes_offset <= all_indexes.size());
        return std::span(all_indexes.begin() + dropped_indexes_offset, all_indexes.end());
    }
    std::vector<IndexID> copyDroppedIndexes() const
    {
        std::vector<IndexID> r;
        for (const auto & id : droppedIndexes())
        {
            r.emplace_back(id);
        }
        return r;
    }

    String toString() const;
};

// Generate a changeset according to `existing_indexes` and `new_table_info`
// If there are newly added or dropped indexes according to `new_table_info`,
// return a changeset with changeset.new_local_index_infos != nullptr
LocalIndexInfosChangeset generateLocalIndexInfos(
    const LocalIndexInfosSnapshot & existing_indexes,
    const TiDB::TableInfo & new_table_info,
    const LoggerPtr & logger);

} // namespace DB::DM
