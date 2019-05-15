#include <Columns/ColumnsNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

static const Field GenDecodeRow(TiDB::CodecFlag flag)
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
    RegionDataReadInfoList & data_list)
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

    ColumnID handle_col_id = InvalidColumnID;

    std::map<ColumnID, std::pair<MutableColumnPtr, NameAndTypePair>> column_map;
    for (const auto & column_info : table_info.columns)
    {
        ColumnID col_id = column_info.id;
        String col_name = column_info.name;
        auto ch_col = columns.getPhysical(col_name);
        column_map[col_id] = std::make_pair(ch_col.type->createColumn(), ch_col);
        column_map[col_id].first->reserve(data_list.size());
        if (table_info.pk_is_handle && column_info.hasPriKeyFlag())
            handle_col_id = col_id;
    }

    if (!table_info.pk_is_handle)
    {
        auto ch_col = columns.getPhysical(MutableSupport::tidb_pk_column_name);
        column_map[handle_col_id] = std::make_pair(ch_col.type->createColumn(), ch_col);
        column_map[handle_col_id].first->reserve(data_list.size());
    }

    bool pk_is_uint64 = false;

    const auto pk_type = column_map[handle_col_id].second.type->getFamilyName();

    if (std::strcmp(pk_type, TypeName<UInt64>::get()) == 0)
        pk_is_uint64 = true;

    if (pk_is_uint64)
    {
        size_t ori_size = data_list.size();
        std::ignore = ori_size;

        // resort the data_list;
        auto it = data_list.begin();
        for (; it != data_list.end();)
        {
            const auto handle = std::get<0>(*it);

            if (handle & RecordKVFormat::SIGN_MARK)
                ++it;
            else
                break;
        }
        data_list.splice(data_list.end(), data_list, data_list.begin(), it);

        assert(ori_size == data_list.size());
    }

    const auto & date_lut = DateLUT::instance();

    ColumnUInt8::Container & delmark_data = delmark_col->getData();
    ColumnUInt64::Container & version_data = version_col->getData();

    delmark_data.reserve(data_list.size());
    version_data.reserve(data_list.size());

    std::unordered_set<ColumnID> col_id_included;

    const size_t target_row_size = (!table_info.pk_is_handle ? table_info.columns.size() : table_info.columns.size() - 1) * 2;

    for (const auto & [handle, write_type, commit_ts, value] : data_list)
    {
        if (write_type == Region::PutFlag || write_type == Region::DelFlag)
        {

            // TODO: optimize columns' insertion, use better implementation rather than Field, it's terrible.

            /// Fill in `row` with decoded column values.

            delmark_data.emplace_back(write_type == Region::DelFlag);
            version_data.emplace_back(commit_ts);

            std::vector<Field> row;

            if (write_type == Region::DelFlag)
            {
                row.reserve(table_info.columns.size() * 2);
                for (const TiDB::ColumnInfo & column : table_info.columns)
                {
                    if (handle_col_id == column.id)
                        continue;

                    row.push_back(Field(column.id));
                    row.push_back(GenDecodeRow(column.getCodecFlag()));
                }
            }
            else
                row = RecordKVFormat::DecodeRow(value);

            if (row.size() == 1 && row[0].isNull())
            {
                // all field is null
                row.clear();
            }

            if (row.size() & 1)
                throw Exception("row size is wrong.", ErrorCodes::LOGICAL_ERROR);

            /// Modify `row` by adding missing column values or removing useless column values.

            col_id_included.clear();
            for (size_t i = 0; i < row.size(); i += 2)
                col_id_included.emplace(row[i].get<ColumnID>());

            // Fill in missing column values.
            for (const TiDB::ColumnInfo & column : table_info.columns)
            {
                if (handle_col_id == column.id)
                    continue;
                if (col_id_included.count(column.id))
                    continue;

                row.push_back(Field(column.id));
                // TODO: Fill `zero` value if NOT NULL specified or else NULL. Need checking DEFAULT VALUE too.
                row.push_back(column.hasNotNullFlag() ? GenDecodeRow(column.getCodecFlag()) : Field());
            }

            // Remove values of non-existing columns, which could be data inserted (but not flushed) before DDLs that drop some columns.
            // TODO: May need to log this.
            for (int i = int(row.size()) - 2; i >= 0; i -= 2)
            {
                Field & col_id = row[i];
                if (column_map.find(col_id.get<ColumnID>()) == column_map.end())
                {
                    row.erase(row.begin() + i, row.begin() + i + 2);
                }
            }

            if (row.size() != target_row_size)
                throw Exception("decode row error.", ErrorCodes::LOGICAL_ERROR);

            /// Transform `row` to columnar format.

            for (size_t i = 0; i < row.size(); i += 2)
            {
                Field & col_id = row[i];
                auto it = column_map.find(col_id.get<ColumnID>());
                if (it == column_map.end())
                    throw Exception("col_id not found in column_map", ErrorCodes::LOGICAL_ERROR);

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

            if (pk_is_uint64)
                column_map[handle_col_id].first->insert(Field(static_cast<UInt64>(handle)));
            else
                column_map[handle_col_id].first->insert(Field(static_cast<Int64>(handle)));
        }
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
