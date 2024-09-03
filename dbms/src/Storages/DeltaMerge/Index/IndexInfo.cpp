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

#include <Storages/DeltaMerge/Index/IndexInfo.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/TiDB.h>

#include <cstddef>

namespace DB::DM
{

bool isVectorIndexSupported(const LoggerPtr & logger)
{
    // Vector Index requires a specific storage format to work.
    if ((STORAGE_FORMAT_CURRENT.identifier > 0 && STORAGE_FORMAT_CURRENT.identifier < 6)
        || STORAGE_FORMAT_CURRENT.identifier == 100)
    {
        LOG_ERROR(
            logger,
            "The current storage format is {}, which does not support building vector index. TiFlash will "
            "write data without vector index.",
            STORAGE_FORMAT_CURRENT.identifier);
        return false;
    }

    return true;
}

TiDB::ColumnInfo getVectorIndxColumnInfo(
    const TiDB::TableInfo & table_info,
    const TiDB::IndexInfo & idx_info,
    const LoggerPtr & logger)
{
    if (!idx_info.vector_index
        || (idx_info.state != TiDB::StatePublic && idx_info.state != TiDB::StateWriteReorganization))
    {
        return {};
    }

    // Vector Index requires a specific storage format to work.
    if (!isVectorIndexSupported(logger))
    {
        return {};
    }

    if (idx_info.idx_cols.size() != 1)
    {
        LOG_ERROR(
            logger,
            "The index columns length is {}, which does not support building vector index, index_id={}, table_id={}.",
            idx_info.idx_cols.size(),
            idx_info.id,
            table_info.id);
        return {};
    }

    for (const auto & col : table_info.columns)
    {
        if (col.name == idx_info.idx_cols[0].name)
        {
            return col;
        }
    }

    LOG_ERROR(
        logger,
        "The index column does not exist, table_id={} index_id={} idx_col_name={}.",
        table_info.id,
        idx_info.id,
        idx_info.idx_cols[0].name);
    return {};
}

LocalIndexInfosPtr initLocalIndexInfos(const TiDB::TableInfo & table_info, const LoggerPtr & logger)
{
    LocalIndexInfosPtr index_infos = std::make_shared<LocalIndexInfos>();
    index_infos->reserve(table_info.columns.size() + table_info.index_infos.size());
    for (const auto & col : table_info.columns)
    {
        if (col.vector_index && isVectorIndexSupported(logger))
        {
            index_infos->emplace_back(LocalIndexInfo{
                .type = IndexType::Vector,
                .column_id = col.id,
                .column_name = col.name,
                .index_definition = col.vector_index,
            });
            LOG_INFO(logger, "Add a new index by column comments, column_id={}, table_id={}.", col.id, table_info.id);
        }
    }

    for (const auto & idx : table_info.index_infos)
    {
        auto column = getVectorIndxColumnInfo(table_info, idx, logger);
        // column.id <= 0 means we don't get the valid column ID.
        if (column.id <= DB::EmptyColumnID)
        {
            LOG_ERROR(
                Logger::get(),
                "The current storage format is {}, which does not support building vector index. TiFlash will "
                "write data without vector index.",
                STORAGE_FORMAT_CURRENT.identifier);
            return {};
        }

        LOG_INFO(logger, "Add a new index, index_id={}, table_id={}.", idx.id, table_info.id);
        index_infos->emplace_back(LocalIndexInfo{
            .type = IndexType::Vector,
            .index_id = idx.id,
            .column_id = column.id,
            .column_name = column.name,
            .index_definition = idx.vector_index,
        });
    }

    index_infos->shrink_to_fit();
    return index_infos;
}

LocalIndexInfosPtr generateLocalIndexInfos(
    const LocalIndexInfosPtr & existing_indexes,
    const TiDB::TableInfo & new_table_info,
    const LoggerPtr & logger)
{
    LocalIndexInfosPtr new_index_infos = std::make_shared<std::vector<LocalIndexInfo>>();
    // The first time generate index infos.
    if (!existing_indexes)
    {
        auto index_infos = initLocalIndexInfos(new_table_info, logger);
        if (index_infos && index_infos->empty())
            return nullptr;
        new_index_infos = std::move(index_infos);
        return new_index_infos;
    }

    new_index_infos->insert(new_index_infos->cend(), existing_indexes->begin(), existing_indexes->end());

    std::unordered_map<IndexID, int> original_local_index_id_map;
    for (size_t index = 0; index < new_index_infos->size(); ++index)
    {
        original_local_index_id_map[new_index_infos->at(index).index_id] = index;
    }

    bool any_new_index_created = false;
    bool any_index_removed = false;
    for (const auto & idx : new_table_info.index_infos)
    {
        if (!idx.vector_index)
            continue;

        auto iter = original_local_index_id_map.find(idx.id);
        if (iter == original_local_index_id_map.end())
        {
            if (idx.state == TiDB::StatePublic || idx.state == TiDB::StateWriteReorganization)
            {
                // create a new index
                auto column = getVectorIndxColumnInfo(new_table_info, idx, logger);
                LocalIndexInfo index_info{
                    .type = IndexType::Vector,
                    .index_id = idx.id,
                    .column_id = column.id,
                    .column_name = column.name,
                    .index_definition = idx.vector_index,
                };
                new_index_infos->emplace_back(std::move(index_info));
                any_new_index_created = true;
                LOG_INFO(logger, "Add a new index, index_id={}, table_id={}.", idx.id, new_table_info.id);
            }
        }
        else
        {
            if (idx.state == TiDB::StateDeleteReorganization)
                continue;
            // remove the existing index
            original_local_index_id_map.erase(iter);
        }
    }

    // drop nonexistent indices
    for (auto & iter : original_local_index_id_map)
    {
        // It means this index is create by column comments which we don't support drop index.
        if (iter.first == DB::EmptyIndexID)
            continue;
        new_index_infos->erase(new_index_infos->begin() + iter.second);
        any_index_removed = true;
        LOG_INFO(logger, "Drop a index, index_id={}, table_id={}.", iter.first, new_table_info.id);
    }

    if (!any_new_index_created && !any_index_removed)
        return nullptr;
    return new_index_infos;
}

} // namespace DB::DM
