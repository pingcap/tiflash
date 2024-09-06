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
#include <Storages/DeltaMerge/Index/IndexInfo.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/TiDB.h>
#include <fiu.h>

namespace DB::FailPoints
{
extern const char force_not_support_vector_index[];
} // namespace DB::FailPoints
namespace DB::DM
{

struct ComplexIndexID
{
    IndexID index_id;
    ColumnID column_id;
};

bool operator==(const ComplexIndexID & lhs, const ComplexIndexID & rhs)
{
    if (lhs.index_id != rhs.index_id)
        return false;
    return lhs.column_id == rhs.column_id;
}

struct ComplexIndexIDHasher
{
    std::size_t operator()(const ComplexIndexID & id) const
    {
        using boost::hash_combine;
        using boost::hash_value;

        std::size_t seed = 0;
        if (id.index_id > EmptyIndexID)
        {
            hash_combine(seed, hash_value(0x01));
            hash_combine(seed, hash_value(id.index_id));
        }
        else
        {
            hash_combine(seed, hash_value(0x02));
            hash_combine(seed, hash_value(id.column_id));
        }
        return seed;
    }
};


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

ColumnID getVectorIndxColumnID(
    const TiDB::TableInfo & table_info,
    const TiDB::IndexInfo & idx_info,
    const LoggerPtr & logger)
{
    if (!idx_info.vector_index)
        return EmptyColumnID;

    // Vector Index requires a specific storage format to work.
    if (unlikely(!isVectorIndexSupported(logger)))
        return EmptyColumnID;

    if (idx_info.idx_cols.size() != 1)
    {
        LOG_ERROR(
            logger,
            "The index columns length is {}, which does not support building vector index, index_id={}, table_id={}.",
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
    return generateLocalIndexInfos(nullptr, table_info, logger);
}

LocalIndexInfosPtr generateLocalIndexInfos(
    const LocalIndexInfosSnapshot & existing_indexes,
    const TiDB::TableInfo & new_table_info,
    const LoggerPtr & logger)
{
    LocalIndexInfosPtr new_index_infos = std::make_shared<LocalIndexInfos>();
    {
        // If the storage format does not support vector index, always return an empty
        // index_info. Meaning we should drop all indexes
        bool is_storage_format_support = isVectorIndexSupported(logger);
        fiu_do_on(FailPoints::force_not_support_vector_index, { is_storage_format_support = false; });
        if (!is_storage_format_support)
            return new_index_infos;
    }

    // Keep a map of "indexes in existing_indexes" -> "offset in new_index_infos"
    std::unordered_map<ComplexIndexID, size_t, ComplexIndexIDHasher> original_local_index_id_map;
    if (existing_indexes)
    {
        // Create a copy of existing indexes
        for (size_t offset = 0; offset < existing_indexes->size(); ++offset)
        {
            const auto & index = (*existing_indexes)[offset];
            original_local_index_id_map.emplace(
                ComplexIndexID{.index_id = index.index_id, .column_id = index.column_id},
                offset);
            new_index_infos->emplace_back(index);
        }
    }

    std::unordered_set<ComplexIndexID, ComplexIndexIDHasher> index_ids_in_new_table;
    std::vector<ComplexIndexID> newly_added;
    std::vector<ComplexIndexID> newly_dropped;

    // In the serverless branch, previously we define vector index on TiDB::ColumnInfo
    for (const auto & col : new_table_info.columns)
    {
        if (!col.vector_index)
            continue;

        // We do the check at the beginning, only assert check under debug mode
        // is enough
        assert(isVectorIndexSupported(logger));

        const ComplexIndexID cindex_id{.index_id = EmptyIndexID, .column_id = col.id};
        index_ids_in_new_table.emplace(cindex_id);
        // already exist in `existing_indexes`
        if (original_local_index_id_map.contains(cindex_id))
            continue;
        // newly added
        new_index_infos->emplace_back(LocalIndexInfo{
            .type = IndexType::Vector,
            .index_id = EmptyIndexID, // the vector index created on ColumnInfo, use EmptyIndexID as the index_id
            .column_id = col.id,
            .index_definition = col.vector_index,
        });
        newly_added.emplace_back(cindex_id);
    }

    for (const auto & idx : new_table_info.index_infos)
    {
        if (!idx.vector_index)
            continue;

        const auto column_id = getVectorIndxColumnID(new_table_info, idx, logger);
        if (column_id <= EmptyColumnID)
            continue;

        const ComplexIndexID cindex_id{.index_id = idx.id, .column_id = column_id};
        auto iter = original_local_index_id_map.find(cindex_id);
        if (iter == original_local_index_id_map.end())
        {
            if (idx.state == TiDB::StatePublic || idx.state == TiDB::StateWriteReorganization)
            {
                // create a new index
                new_index_infos->emplace_back(LocalIndexInfo{
                    .type = IndexType::Vector,
                    .index_id = idx.id,
                    .column_id = column_id,
                    .index_definition = idx.vector_index,
                });
                newly_added.emplace_back(cindex_id);
                index_ids_in_new_table.emplace(cindex_id);
            }
            // else the index is not public or write reorg, consider this index as not exist
        }
        else
        {
            if (idx.state != TiDB::StateDeleteReorganization)
                index_ids_in_new_table.emplace(cindex_id);
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
        auto get_logging = [&]() -> String {
            FmtBuffer buf;
            buf.append("keep=[");
            buf.joinStr(
                original_local_index_id_map.begin(),
                original_local_index_id_map.end(),
                [](const auto & id, FmtBuffer & fb) {
                    if (id.first.index_id != EmptyIndexID)
                        fb.fmtAppend("index_id={}", id.first.index_id);
                    else
                        fb.fmtAppend("index_on_column_id={}", id.first.column_id);
                },
                ",");
            buf.append("]");
            return buf.toString();
        };
        LOG_DEBUG(logger, "Local index info does not changed, {}", get_logging());
        return nullptr;
    }

    auto get_changed_logging = [&]() -> String {
        FmtBuffer buf;
        buf.append("keep=[");
        buf.joinStr(
            original_local_index_id_map.begin(),
            original_local_index_id_map.end(),
            [](const auto & id, FmtBuffer & fb) {
                if (id.first.index_id != EmptyIndexID)
                    fb.fmtAppend("index_id={}", id.first.index_id);
                else
                    fb.fmtAppend("index_on_column_id={}", id.first.column_id);
            },
            ",");
        buf.append("] added=[");
        buf.joinStr(
            newly_added.begin(),
            newly_added.end(),
            [](const ComplexIndexID & id, FmtBuffer & fb) {
                if (id.index_id != EmptyIndexID)
                    fb.fmtAppend("index_id={}", id.index_id);
                else
                    fb.fmtAppend("index_on_column_id={}", id.column_id);
            },
            ",");
        buf.append("] dropped=[");
        buf.joinStr(
            newly_dropped.begin(),
            newly_dropped.end(),
            [](const ComplexIndexID & id, FmtBuffer & fb) {
                if (id.index_id != EmptyIndexID)
                    fb.fmtAppend("index_id={}", id.index_id);
                else
                    fb.fmtAppend("index_on_column_id={}", id.column_id);
            },
            ",");
        buf.append("]");
        return buf.toString();
    };
    LOG_INFO(logger, "Local index info generated, {}", get_changed_logging());
    return new_index_infos;
}

} // namespace DB::DM
