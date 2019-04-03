#include <Storages/Transaction/RegionBlockReader.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

static const Field MockDecodeRow(TiDB::CodecFlag flag)
{
    switch (flag)
    {
        case TiDB::CodecFlagNil:
            return Field();
        case TiDB::CodecFlagBytes:
            return Field(String());
        case TiDB::CodecFlagDecimal:
            return Field(Decimal(0));
        case TiDB::CodecFlagCompactBytes:
            return Field(String());
        case TiDB::CodecFlagFloat:
            return Field(Float64(0));
        case TiDB::CodecFlagUInt:
            return Field(UInt64(0));
        case TiDB::CodecFlagInt:
            return Field(Int64(0));
        case TiDB::CodecFlagVarInt:
            return Field(Int64(0));
        case TiDB::CodecFlagVarUInt:
            return Field(UInt64(0));
        default:
            throw Exception("Not implented codec flag: " + std::to_string(flag), ErrorCodes::LOGICAL_ERROR);
    }
}

Block RegionBlockRead(const TiDB::TableInfo & table_info, const ColumnsDescription & columns, const Names & ordered_columns_,
    ScannerPtr & scanner, std::vector<RegionWriteCFData::Key> * keys)
{
    // Note: this code below is mostly ported from RegionBlockInputStream.
    Names ordered_columns = ordered_columns_;
    if (ordered_columns_.empty())
    {
        for (const auto & col : columns.ordinary)
            ordered_columns.push_back(col.name);
    }

    auto delmark_col = ColumnUInt8::create();
    auto version_col = ColumnUInt64::create();
    // TODO: use HandleType and InvalidHandleID
    Int64 handle_id = -1;

    std::map<UInt64, std::pair<MutableColumnPtr, NameAndTypePair>> column_map;
    for (const auto & column_info : table_info.columns)
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
        auto ch_col = columns.getPhysical(MutableSupport::tidb_pk_column_name);
        column_map[handle_id] = std::make_pair(ch_col.type->createColumn(), ch_col);
    }

    const auto & date_lut = DateLUT::instance();

    // TODO: lock region, to avoid region adding/droping while writing data

    TableID my_table_id = table_info.id;
    TableID next_table_id = InvalidTableID;

    // Here we use do{}while() instead of while(){},
    // Because the first check of scanner.hasNext() already been done outside of this function.
    while (true)
    {
        // TODO: comfirm all this mess
        auto [handle, write_type, commit_ts, value] = scanner->next(keys);
        if (write_type == Region::PutFlag || write_type == Region::DelFlag)
        {

            // TODO: optimize columns' insertion

            ColumnUInt8::Container & delmark_data = delmark_col->getData();
            ColumnUInt64::Container & version_data = version_col->getData();

            // `write_type` does not equal `Op` in proto
            delmark_data.resize(delmark_data.size() + 1);
            delmark_data[delmark_data.size() - 1] = write_type == Region::DelFlag;

            version_data.resize(version_data.size() + 1);
            version_data[version_data.size() - 1] = commit_ts;

            std::vector<Field> row;

            if (write_type == Region::DelFlag)
            {
                row.reserve(table_info.columns.size() * 2);
                for (const TiDB::ColumnInfo & column : table_info.columns)
                {
                    row.push_back(Field(column.id));
                    row.push_back(MockDecodeRow(column.getCodecFlag()));
                }
            }
            else
                row = RecordKVFormat::DecodeRow(value);

            if (row.size() % 2 != 0)
                throw Exception("The number of columns is not right!", ErrorCodes::LOGICAL_ERROR);

            for (size_t i = 0; i < row.size(); i += 2)
            {
                Field & col_id = row[i];
                auto it = column_map.find(col_id.get<Int64>());
                if (it == column_map.end())
                    continue;
                const auto & tp = it->second.second.type->getName();
                if (tp == "Nullable(DateTime)" || tp == "Nullable(Date)" || tp == "DateTime" || tp == "Date")
                {
                    Field & field = row[i + 1];
                    UInt64 packed = field.get<Int64>();
                    UInt64 ymdhms = packed >> 24;
                    UInt64 ymd = ymdhms >> 17;
                    int day = int(ymd & ((1 << 5) - 1));
                    int ym = ymd >> 5;
                    int month = int(ym % 13);
                    int year = int(ym / 13);

                    UInt64 hms = ymdhms & ((1 << 17) - 1);
                    int second = int(hms & ((1 << 6) - 1));
                    int minute = int((hms >> 6) & ((1 << 6) - 1));
                    int hour = int(hms >> 12);

                    if (tp == "Nullable(DateTime)" || tp == "DataTime")
                    {
                        time_t datetime;
                        if (unlikely(year == 0))
                            datetime = 0;
                        else
                            datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);
                        it->second.first->insert(static_cast<Int64>(datetime));
                    }
                    else
                    {
                        auto date = date_lut.makeDayNum(year, month, day);
                        Field date_field(static_cast<Int64>(date));
                        it->second.first->insert(date_field);
                    }
                }
                else
                {
                    it->second.first->insert(row[i + 1]);
                }
            }
            column_map[handle_id].first->insert(Field(handle));
        }

        next_table_id = scanner->hasNext();

        if (next_table_id == InvalidTableID || next_table_id != my_table_id)
            break;
    }

    Block block;
    for (const auto & name : ordered_columns)
    {
        if (name == MutableSupport::delmark_column_name)
        {
            block.insert({std::move(delmark_col), std::make_shared<DataTypeUInt8>(), MutableSupport::delmark_column_name});
        }
        else if (name == MutableSupport::version_column_name)
        {
            block.insert({std::move(version_col), std::make_shared<DataTypeUInt64>(), MutableSupport::version_column_name});
        }
        else
        {
            Int64 col_id = table_info.getColumnID(name);
            block.insert({std::move(column_map[col_id].first), column_map[col_id].second.type, name});
        }
    }

    return block;
}

} // namespace DB
