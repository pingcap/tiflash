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

#include <Common/FmtUtils.h>
#include <Common/config.h> // For ENABLE_CLARA
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/TiDB.h>
#include <fiu.h>

namespace DB::FailPoints
{
extern const char force_not_support_local_index[];
} // namespace DB::FailPoints
namespace DB::DM
{

bool isLocalIndexSupported(const LoggerPtr & logger)
{
    if ((STORAGE_FORMAT_CURRENT.identifier > 0 && STORAGE_FORMAT_CURRENT.identifier < 6)
        || STORAGE_FORMAT_CURRENT.identifier == 100)
    {
        LOG_ERROR(
            logger,
            "The current storage format is {}, which does not support building columnar index "
            "like vector index or full text index. TiFlash will write data without index.",
            STORAGE_FORMAT_CURRENT.identifier);
        return false;
    }

    return true;
}

ColumnID getColumnarIndexColumnID(
    const TiDB::TableInfo & table_info,
    const TiDB::IndexInfo & idx_info,
    const LoggerPtr & logger)
{
    if (!idx_info.isColumnarIndex())
        return EmptyColumnID;

    // Vector Index requires a specific storage format to work.
    if (unlikely(!isLocalIndexSupported(logger)))
        return EmptyColumnID;

    if (idx_info.idx_cols.size() != 1)
    {
        LOG_ERROR(
            logger,
            "The index columns length is {}, which does not support building local index, index_id={}, table_id={}.",
            idx_info.idx_cols.size(),
            idx_info.id,
            table_info.id);
        return EmptyColumnID;
    }

    for (const auto & col : table_info.columns)
    {
        if (col.name == idx_info.idx_cols[0].name)
        {
            return col.id;
        }
    }

    LOG_ERROR(
        logger,
        "The index column does not exist, table_id={} index_id={} idx_col_name={}.",
        table_info.id,
        idx_info.id,
        idx_info.idx_cols[0].name);
    return EmptyColumnID;
}

LocalIndexInfosPtr initLocalIndexInfos(const TiDB::TableInfo & table_info, const LoggerPtr & logger)
{
    // The same as generate local index infos with no existing_indexes
    return generateLocalIndexInfos(nullptr, table_info, logger).new_local_index_infos;
}

namespace
{
LocalIndexInfosChangeset nothingChanged(const std::unordered_map<IndexID, size_t> & original_local_index_id_map)
{
    std::vector<IndexID> all_indexes;
    all_indexes.reserve(original_local_index_id_map.size());
    for (const auto & it : original_local_index_id_map)
        all_indexes.emplace_back(it.first);
    size_t end_offset = all_indexes.size();
    return LocalIndexInfosChangeset{
        .new_local_index_infos = nullptr,
        .all_indexes = std::move(all_indexes),
        .added_indexes_offset = end_offset,
        .dropped_indexes_offset = end_offset,
    };
}
LocalIndexInfosChangeset getChangeset(
    LocalIndexInfosPtr && new_index_infos,
    const std::unordered_map<IndexID, size_t> & original_local_index_id_map,
    const std::vector<IndexID> & newly_added,
    const std::vector<IndexID> & newly_dropped)
{
    std::vector<IndexID> all_indexes;
    all_indexes.reserve(original_local_index_id_map.size() + newly_added.size() + newly_dropped.size());
    for (const auto & it : original_local_index_id_map)
        all_indexes.emplace_back(it.first);

    const size_t added_begin_offset = all_indexes.size();
    for (const auto & id : newly_added)
        all_indexes.emplace_back(id);

    const size_t dropped_begin_offset = all_indexes.size();
    for (const auto & id : newly_dropped)
        all_indexes.emplace_back(id);

    return LocalIndexInfosChangeset{
        .new_local_index_infos = std::move(new_index_infos),
        .all_indexes = std::move(all_indexes),
        .added_indexes_offset = added_begin_offset,
        .dropped_indexes_offset = dropped_begin_offset,
    };
}
} // namespace

LocalIndexInfosChangeset generateLocalIndexInfos(
    const LocalIndexInfosSnapshot & existing_indexes,
    const TiDB::TableInfo & new_table_info,
    const LoggerPtr & logger)
{
    LocalIndexInfosPtr new_index_infos = std::make_shared<LocalIndexInfos>();
    {
        // If the storage format does not support vector index, always return an empty
        // index_info. Meaning we should drop all indexes
        bool is_storage_format_support = isLocalIndexSupported(logger);
        fiu_do_on(FailPoints::force_not_support_local_index, { is_storage_format_support = false; });
        if (!is_storage_format_support)
            return LocalIndexInfosChangeset{
                .new_local_index_infos = new_index_infos,
            };
    }

    // Keep a map of "indexes in existing_indexes" -> "offset in new_index_infos"
    std::unordered_map<IndexID, size_t> original_local_index_id_map;
    if (existing_indexes)
    {
        // Create a copy of existing indexes
        for (size_t offset = 0; offset < existing_indexes->size(); ++offset)
        {
            const auto & index = (*existing_indexes)[offset];
            original_local_index_id_map.emplace(index.index_id, offset);
            new_index_infos->emplace_back(index);
        }
    }

    std::unordered_set<IndexID> index_ids_in_new_table;
    std::vector<IndexID> newly_added;
    std::vector<IndexID> newly_dropped;

    for (const auto & idx : new_table_info.index_infos)
    {
        if (!idx.isColumnarIndex())
            continue;

        const auto column_id = getColumnarIndexColumnID(new_table_info, idx, logger);
        if (column_id <= EmptyColumnID)
            continue;

        if (!original_local_index_id_map.contains(idx.id))
        {
            if (idx.state == TiDB::StatePublic || idx.state == TiDB::StateWriteReorganization)
            {
                // create a new index
                if (idx.columnarIndexKind() == TiDB::ColumnarIndexKind::Vector)
                    new_index_infos->emplace_back(LocalIndexInfo(idx.id, column_id, idx.vector_index));
#if ENABLE_CLARA
                else if (idx.columnarIndexKind() == TiDB::ColumnarIndexKind::FullText)
                    new_index_infos->emplace_back(LocalIndexInfo(idx.id, column_id, idx.full_text_index));
#endif
                else if (idx.columnarIndexKind() == TiDB::ColumnarIndexKind::Inverted)
                    new_index_infos->emplace_back(LocalIndexInfo(idx.id, column_id, idx.inverted_index));
                newly_added.emplace_back(idx.id);
                index_ids_in_new_table.emplace(idx.id);
            }
            // else the index is not public or write reorg, consider this index as not exist
        }
        else
        {
            if (idx.state != TiDB::StateDeleteReorganization)
                index_ids_in_new_table.emplace(idx.id);
            // else exist in both `existing_indexes` and `new_table_info`, but enter "delete reorg". We consider this
            // index as not exist in the `new_table_info` and drop it later
        }
    }

    // drop nonexistent indexes
    for (auto iter = original_local_index_id_map.begin(); iter != original_local_index_id_map.end(); /* empty */)
    {
        // the index_id exists in both `existing_indexes` and `new_table_info`
        if (index_ids_in_new_table.contains(iter->first))
        {
            ++iter;
            continue;
        }

        // not exists in `new_table_info`, drop it
        newly_dropped.emplace_back(iter->first);
        new_index_infos->erase(new_index_infos->begin() + iter->second);
        iter = original_local_index_id_map.erase(iter);
    }

    if (newly_added.empty() && newly_dropped.empty())
    {
        return nothingChanged(original_local_index_id_map);
    }

    return getChangeset(std::move(new_index_infos), original_local_index_id_map, newly_added, newly_dropped);
}

String LocalIndexInfosChangeset::toString() const
{
    FmtBuffer buf;
    buf.append("keep=[");
    auto keep_indexes = keepIndexes();
    buf.joinStr(
        keep_indexes.begin(),
        keep_indexes.end(),
        [](const auto & id, FmtBuffer & fb) { fb.fmtAppend("index_id={}", id); },
        ",");
    buf.append("]");

    auto added_indexes = addedIndexes();
    if (new_local_index_infos != nullptr || !added_indexes.empty())
    {
        buf.append(" added=[");
        buf.joinStr(
            added_indexes.begin(),
            added_indexes.end(),
            [](const auto & id, FmtBuffer & fb) { fb.fmtAppend("index_id={}", id); },
            ",");
        buf.append("]");
    }
    auto dropped_indexes = droppedIndexes();
    if (new_local_index_infos != nullptr || !dropped_indexes.empty())
    {
        buf.append(" dropped=[");
        buf.joinStr(
            dropped_indexes.begin(),
            dropped_indexes.end(),
            [](const auto & id, FmtBuffer & fb) { fb.fmtAppend("index_id={}", id); },
            ",");
        buf.append("]");
    }
    return buf.toString();
}

} // namespace DB::DM
