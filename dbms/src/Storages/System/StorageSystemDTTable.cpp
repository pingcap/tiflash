#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/System/StorageSystemDTTables.h>
#include <Storages/MutableSupport.h>

namespace DB
{

StorageSystemDTTables::StorageSystemDTTables(const std::string & name_) : name(name_)
{
    setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"table_id", std::make_shared<DataTypeInt64>()},

        {"segment_count", std::make_shared<DataTypeUInt64>()},

        {"total_rows", std::make_shared<DataTypeUInt64>()},
        {"total_size", std::make_shared<DataTypeUInt64>()},
        {"total_delete_ranges", std::make_shared<DataTypeUInt64>()},

        {"delta_rate_rows", std::make_shared<DataTypeFloat64>()},
        {"delta_rate_segments", std::make_shared<DataTypeFloat64>()},

        {"delta_placed_rate", std::make_shared<DataTypeFloat64>()},
        {"delta_cache_size", std::make_shared<DataTypeUInt64>()},
        {"delta_cache_rate", std::make_shared<DataTypeFloat64>()},
        {"delta_cache_wasted_rate", std::make_shared<DataTypeFloat64>()},

        {"avg_segment_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_segment_size", std::make_shared<DataTypeFloat64>()},

        {"delta_count", std::make_shared<DataTypeUInt64>()},
        {"total_delta_rows", std::make_shared<DataTypeUInt64>()},
        {"total_delta_size", std::make_shared<DataTypeUInt64>()},
        {"avg_delta_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_delta_size", std::make_shared<DataTypeFloat64>()},
        {"avg_delta_delete_ranges", std::make_shared<DataTypeFloat64>()},

        {"stable_count", std::make_shared<DataTypeUInt64>()},
        {"total_stable_rows", std::make_shared<DataTypeUInt64>()},
        {"total_stable_size", std::make_shared<DataTypeUInt64>()},
        {"avg_stable_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_stable_size", std::make_shared<DataTypeFloat64>()},

        {"total_pack_count_in_delta", std::make_shared<DataTypeUInt64>()},
        {"avg_pack_count_in_delta", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_rows_in_delta", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_size_in_delta", std::make_shared<DataTypeFloat64>()},

        {"total_pack_count_in_stable", std::make_shared<DataTypeUInt64>()},
        {"avg_pack_count_in_stable", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_rows_in_stable", std::make_shared<DataTypeFloat64>()},
        {"avg_pack_size_in_stable", std::make_shared<DataTypeFloat64>()},

        {"storage_stable_num_snapshots", std::make_shared<DataTypeUInt64>()},
        {"storage_stable_num_pages", std::make_shared<DataTypeUInt64>()},
        {"storage_stable_num_normal_pages", std::make_shared<DataTypeUInt64>()},
        {"storage_stable_max_page_id", std::make_shared<DataTypeUInt64>()},

        {"storage_delta_num_snapshots", std::make_shared<DataTypeUInt64>()},
        {"storage_delta_num_pages", std::make_shared<DataTypeUInt64>()},
        {"storage_delta_num_normal_pages", std::make_shared<DataTypeUInt64>()},
        {"storage_delta_max_page_id", std::make_shared<DataTypeUInt64>()},

        {"storage_meta_num_snapshots", std::make_shared<DataTypeUInt64>()},
        {"storage_meta_num_pages", std::make_shared<DataTypeUInt64>()},
        {"storage_meta_num_normal_pages", std::make_shared<DataTypeUInt64>()},
        {"storage_meta_max_page_id", std::make_shared<DataTypeUInt64>()},

        {"background_tasks_length", std::make_shared<DataTypeUInt64>()},
    }));
}


BlockInputStreams StorageSystemDTTables::read(const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    auto databases = context.getDatabases();
    for (const auto & d : databases)
    {
        String database_name = d.first;
        auto & database = d.second;
        auto it = database->getIterator(context);
        for (; it->isValid(); it->next())
        {
            auto & table_name = it->name();
            auto & storage = it->table();
            if (storage->getName() != MutableSupport::delta_tree_storage_name)
                continue;

            auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            auto table_id = dm_storage->getTableInfo().id;
            auto stat = dm_storage->getStore()->getStat();

            res_columns[0]->insert(database_name);
            res_columns[1]->insert(table_name);
            res_columns[2]->insert(table_id);

            res_columns[3]->insert(stat.segment_count);

            res_columns[4]->insert(stat.total_rows);
            res_columns[5]->insert(stat.total_size);
            res_columns[6]->insert(stat.total_delete_ranges);

            res_columns[7]->insert(stat.delta_rate_rows);
            res_columns[8]->insert(stat.delta_rate_segments);

            res_columns[9]->insert(stat.delta_placed_rate);
            res_columns[10]->insert(stat.delta_cache_size);
            res_columns[11]->insert(stat.delta_cache_rate);
            res_columns[12]->insert(stat.delta_cache_wasted_rate);

            res_columns[13]->insert(stat.avg_segment_rows);
            res_columns[14]->insert(stat.avg_segment_size);

            res_columns[15]->insert(stat.delta_count);
            res_columns[16]->insert(stat.total_delta_rows);
            res_columns[17]->insert(stat.total_delta_size);
            res_columns[18]->insert(stat.avg_delta_rows);
            res_columns[19]->insert(stat.avg_delta_size);
            res_columns[20]->insert(stat.avg_delta_delete_ranges);

            res_columns[21]->insert(stat.stable_count);
            res_columns[22]->insert(stat.total_stable_rows);
            res_columns[23]->insert(stat.total_stable_size);
            res_columns[24]->insert(stat.avg_stable_rows);
            res_columns[25]->insert(stat.avg_stable_size);

            res_columns[26]->insert(stat.total_pack_count_in_delta);
            res_columns[27]->insert(stat.avg_pack_count_in_delta);
            res_columns[28]->insert(stat.avg_pack_rows_in_delta);
            res_columns[29]->insert(stat.avg_pack_size_in_delta);

            res_columns[30]->insert(stat.total_pack_count_in_stable);
            res_columns[31]->insert(stat.avg_pack_count_in_stable);
            res_columns[32]->insert(stat.avg_pack_rows_in_stable);
            res_columns[33]->insert(stat.avg_pack_size_in_stable);

            res_columns[34]->insert(stat.storage_stable_num_snapshots);
            res_columns[35]->insert(stat.storage_stable_num_pages);
            res_columns[36]->insert(stat.storage_stable_num_normal_pages);
            res_columns[37]->insert(stat.storage_stable_max_page_id);

            res_columns[38]->insert(stat.storage_delta_num_snapshots);
            res_columns[39]->insert(stat.storage_delta_num_pages);
            res_columns[40]->insert(stat.storage_delta_num_normal_pages);
            res_columns[41]->insert(stat.storage_delta_max_page_id);

            res_columns[42]->insert(stat.storage_meta_num_snapshots);
            res_columns[43]->insert(stat.storage_meta_num_pages);
            res_columns[44]->insert(stat.storage_meta_num_normal_pages);
            res_columns[45]->insert(stat.storage_meta_max_page_id);

            res_columns[46]->insert(stat.background_tasks_length);
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB
