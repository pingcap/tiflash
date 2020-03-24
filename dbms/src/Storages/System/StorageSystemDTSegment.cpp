#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/System/StorageSystemDTSegments.h>
#include <Storages/MutableSupport.h>

namespace DB
{
StorageSystemDTSegments::StorageSystemDTSegments(const std::string & name_) : name(name_)
{
    setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"table_id", std::make_shared<DataTypeInt64>()},

        {"segment_id", std::make_shared<DataTypeUInt64>()},
        {"range", std::make_shared<DataTypeString>()},

        {"rows", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"delete_ranges", std::make_shared<DataTypeUInt64>()},

        {"delta_pack_count", std::make_shared<DataTypeUInt64>()},
        {"stable_pack_count", std::make_shared<DataTypeUInt64>()},

        {"avg_delta_pack_rows", std::make_shared<DataTypeFloat64>()},
        {"avg_stable_pack_rows", std::make_shared<DataTypeFloat64>()},

        {"delta_rate", std::make_shared<DataTypeFloat64>()},
        {"delta_cache_size", std::make_shared<DataTypeUInt64>()},
    }));
}

BlockInputStreams StorageSystemDTSegments::read(const Names & column_names,
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
            auto segment_stats = dm_storage->getStore()->getSegmentStats();
            for (auto & stat : segment_stats)
            {
                res_columns[0]->insert(database_name);
                res_columns[1]->insert(table_name);
                res_columns[2]->insert(table_id);

                res_columns[3]->insert(stat.segment_id);
                res_columns[4]->insert(stat.range.toString());
                res_columns[5]->insert(stat.rows);
                res_columns[6]->insert(stat.size);
                res_columns[7]->insert(stat.delete_ranges);

                res_columns[8]->insert(stat.delta_pack_count);
                res_columns[9]->insert(stat.stable_pack_count);

                res_columns[10]->insert(stat.avg_delta_pack_rows);
                res_columns[11]->insert(stat.avg_stable_pack_rows);

                res_columns[12]->insert(stat.delta_rate);
                res_columns[13]->insert(stat.delta_cache_size);
            }
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB
