// TODO: totally remove

/*
#include <DataStreams/RegionBlockInputStream.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/MutableSupport.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <IO/ReadHelpers.h>
#include <Common/typeid_cast.h>

namespace DB {

Block RegionBlockInputStream::readImpl()
{
    auto delmark_col = ColumnUInt8::create();
    auto version_col = ColumnUInt64::create();
    Int64 handle_id  = -1;
    std::map<UInt64, std::pair<MutableColumnPtr, NameAndTypePair> > column_map;
    for (auto column_info : table_info.columns)
    {
        Int64 col_id = column_info.id;
        String col_name = column_info.name;
        auto ch_col = columns.getPhysical(col_name);
        column_map[col_id] = std::make_pair(ch_col.type->createColumn(), ch_col);
        if (table_info.pk_is_handle && column_info.hasPriKeyFlag())
        {
            handle_id = col_id;
        }
    }
    if (!table_info.pk_is_handle)
    {
        auto ch_col = columns.getPhysical("_tidb_rowid");
        column_map[handle_id] = std::make_pair(ch_col.type->createColumn(), ch_col);
    }

    const auto & date_lut = DateLUT::instance();

    // TODO: lock partition, to avoid region adding/droping while writing data
    // TODO: use `Partition & partition` instead of `Partition partition`
    RegionPartitionMgr::Partition partition = table_partitions->getPartition(partition_id);
    for (auto it = partition.regions.begin(); it != partition.regions.end(); ++it)
    {
        auto remover = it->second->createCommittedScanRemover();
        while (remover->hasNext())
        {
            // TODO: comfirm all this mess
            auto [handle, write_type, commit_ts, value] = remover->next();

            if (write_type == Region::LockFlag || write_type == Region::RollbackFlag)
            {
                continue;
            }

            // TODO: optimize columns' insertion

            ColumnUInt8::Container & delmark_data = delmark_col->getData();
            ColumnUInt64::Container & version_data = version_col->getData();

            // `write_type` does not equal `Op` in proto
            delmark_data.resize(delmark_data.size() + 1);
            delmark_data[delmark_data.size() - 1] = write_type == Region::DelFlag;

            version_data.resize(version_data.size() + 1);
            version_data[version_data.size() - 1] = commit_ts;

            std::vector<Field> row = DecodeRow(value);
            if (row.size() % 2 != 0) {
                throw Exception("the number of columns is not right!");
            }

            for (size_t i = 0; i < row.size(); i += 2)
            {
                Field & col_id = row[i];
                auto it = column_map.find(col_id.get<Int64>());
                if (it == column_map.end())
                    continue;
                if (typeid_cast<const DataTypeDateTime* >(it->second.second.type.get()) || typeid_cast<const DataTypeDate* >(it->second.second.type.get())) {
                    Field & field = row[i+1];
                    UInt64 packed = field.get<Int64>();
                    UInt64 ymdhms = packed >> 24;
                    UInt64 ymd = ymdhms >> 17;
                    int day = int(ymd & ((1<<5) - 1));
                    int ym = ymd >> 5 ;
                    int month = int(ym % 13);
                    int year = int(ym / 13);

                    UInt64 hms = ymdhms & ((1<<17) - 1) ;
                    int second = int(hms & ((1<<6) - 1));
                    int minute = int((hms >> 6) & ((1<<6) - 1));
                    int hour = int(hms >> 12);

                    if (typeid_cast<const DataTypeDateTime* >(it->second.second.type.get())) {
                        time_t datetime;
                        if (unlikely(year == 0))
                            datetime = 0;
                        else
                            datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);
                        it -> second.first -> insert(static_cast<Int64>(datetime));
                    } else {
                        DayNum_t date = date_lut.makeDayNum(year, month, day);
                        it -> second.first -> insert((UInt64)date.t);
                    }
                } else {
                    it -> second.first -> insert(row[i+1]);
                }
            }
            column_map[handle_id].first -> insert(Field(handle));
        }
        remover->remove(table_info.id);
    }

    Block block;
    for (String name: ordered_columns)
    {
        if (name == MutableSupport::delmark_column_name)
        {
            block.insert({std::move(delmark_col), std::make_shared<DataTypeUInt8>(), MutableSupport::delmark_column_name});
        } else if (name == MutableSupport::version_column_name)
        {
            block.insert({std::move(version_col), std::make_shared<DataTypeUInt64>(), MutableSupport::version_column_name});
        } else {
            Int64 col_id = table_info.getColumnID(name);
            block.insert({std::move(column_map[col_id].first), column_map[col_id].second.type, name});
        }
    }

    return block;
}

}
*/
