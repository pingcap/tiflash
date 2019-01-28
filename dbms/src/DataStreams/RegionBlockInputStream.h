// TODO: totally remove
/*
#pragma once
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/RegionPartitionMgr.h>

namespace DB
{

struct RegionBlockInputStream : public IProfilingBlockInputStream
{
public :
    RegionBlockInputStream(UInt64                     partition_id_,
                           RegionPartitionMgrPtr      table_partitions_,
                           const TiDB::TableInfo &    table_info_,
                           const ColumnsDescription & columns_,
                           const Names &              ordered_columns_)
        : partition_id(partition_id_),
          table_partitions(table_partitions_),
          table_info(table_info_),
          columns(columns_),
          ordered_columns(ordered_columns_)
    {
        if (ordered_columns.size() == 0)
        {
            for (auto col : columns.ordinary) {
                ordered_columns.push_back(col.name);
            }
        }
    }

    String getName() const override { return "RegionInput"; }

    // TODO: refine this
    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

    UInt64 partition_id;
    RegionPartitionMgrPtr table_partitions;
    const TiDB::TableInfo & table_info;
    const ColumnsDescription & columns;
    Names ordered_columns;

    Block header;
};

}
*/
