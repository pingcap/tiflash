#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/TMTPKType.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
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
            throw Exception("Not implemented codec flag: " + std::to_string(flag), ErrorCodes::LOGICAL_ERROR);
    }
}

inline void ReorderRegionDataReadList(RegionDataReadInfoList & data_list)
{
    // resort the data_list
    // if the order in int64 is like -3 -1 0 1 2 3, the real order in uint64 is 0 1 2 3 -3 -1
    if (data_list.size() > 2)
    {
        bool need_check = false;
        {
            const auto h1 = std::get<0>(data_list.front());
            const auto h2 = std::get<0>(data_list.back());
            if ((h1 ^ h2) & RecordKVFormat::SIGN_MARK)
                need_check = true;
        }

        if (need_check)
        {
            auto it = data_list.begin();
            for (; it != data_list.end();)
            {
                const auto handle = std::get<0>(*it);

                if (handle & RecordKVFormat::SIGN_MARK)
                    ++it;
                else
                    break;
            }

            std::reverse(it, data_list.end());
            std::reverse(data_list.begin(), it);
            std::reverse(data_list.begin(), data_list.end());
        }
    }
}

template <TMTPKType pk_type>
void setPKVersionDel(ColumnUInt8 & delmark_col,
    ColumnUInt64 & version_col,
    MutableColumnPtr & pk_column,
    const RegionDataReadInfoList & data_list,
    const Timestamp tso)
{
    ColumnUInt8::Container & delmark_data = delmark_col.getData();
    ColumnUInt64::Container & version_data = version_col.getData();

    delmark_data.reserve(data_list.size());
    version_data.reserve(data_list.size());

    for (const auto & [handle, write_type, commit_ts, value] : data_list)
    {
        std::ignore = value;

        // Ignore data after the start_ts.
        if (commit_ts > tso)
            continue;

        delmark_data.emplace_back(write_type == Region::DelFlag);
        version_data.emplace_back(commit_ts);

        if constexpr (pk_type == TMTPKType::INT64)
            typeid_cast<ColumnVector<Int64> &>(*pk_column).insert(static_cast<Int64>(handle));
        else if constexpr (pk_type == TMTPKType::UINT64)
            typeid_cast<ColumnVector<UInt64> &>(*pk_column).insert(static_cast<UInt64>(handle));
        else
            pk_column->insert(Field(static_cast<Int64>(handle)));
    }
}

std::tuple<Block, bool> readRegionBlock(const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & column_names_to_read,
    RegionDataReadInfoList & data_list,
    Timestamp start_ts,
    bool force_decode)
{
    auto delmark_col = ColumnUInt8::create();
    auto version_col = ColumnUInt64::create();

    ColumnID handle_col_id = InvalidColumnID;

    std::unordered_map<ColumnID, std::pair<MutableColumnPtr, NameAndTypePair>> column_map;
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

    const TMTPKType pk_type = getTMTPKType(*column_map[handle_col_id].second.type);

    if (pk_type == TMTPKType::UINT64)
        ReorderRegionDataReadList(data_list);

    {
        auto func = setPKVersionDel<TMTPKType::UNSPECIFIED>;

        switch (pk_type)
        {
            case TMTPKType::INT64:
                func = setPKVersionDel<TMTPKType::INT64>;
                break;
            case TMTPKType::UINT64:
                func = setPKVersionDel<TMTPKType::UINT64>;
                break;
            default:
                break;
        }

        func(*delmark_col, *version_col, column_map[handle_col_id].first, data_list, start_ts);
    }

    const auto & date_lut = DateLUT::instance();

    std::unordered_set<ColumnID> col_id_included;

    const size_t target_col_size = (!table_info.pk_is_handle ? table_info.columns.size() : table_info.columns.size() - 1) * 2;

    Block block;

    // optimize for only need handle, tso, delmark.
    if (column_names_to_read.size() > 3)
    {
        for (const auto & [handle, write_type, commit_ts, value_ptr] : data_list)
        {
            std::ignore = handle;

            // Ignore data after the start_ts.
            if (commit_ts > start_ts)
                continue;

            // TODO: optimize columns' insertion, use better implementation rather than Field, it's terrible.

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
                row = RecordKVFormat::DecodeRow(*value_ptr);

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

                if (!force_decode)
                    return std::make_tuple(block, false);

                row.emplace_back(Field(column.id));
                if (column.hasNoDefaultValueFlag())
                    // Fill `zero` value if NOT NULL specified or else NULL.
                    row.push_back(column.hasNotNullFlag() ? GenDecodeRow(column.getCodecFlag()) : Field());
                else
                    // Fill default value.
                    row.push_back(column.defaultValueToField());
            }

            // Remove values of non-existing columns, which could be data inserted (but not flushed) before DDLs that drop some columns.
            // TODO: May need to log this.
            for (int i = int(row.size()) - 2; i >= 0; i -= 2)
            {
                Field & col_id = row[i];
                if (column_map.find(col_id.get<ColumnID>()) == column_map.end())
                {
                    if (!force_decode)
                        return std::make_tuple(block, false);

                    row.erase(row.begin() + i, row.begin() + i + 2);
                }
            }

            if (row.size() != target_col_size)
                throw Exception("decode row error.", ErrorCodes::LOGICAL_ERROR);

            /// Transform `row` to columnar format.

            for (size_t i = 0; i < row.size(); i += 2)
            {
                Field & col_id = row[i];
                auto it = column_map.find(col_id.get<ColumnID>());
                if (it == column_map.end())
                    throw Exception("col_id not found in column_map", ErrorCodes::LOGICAL_ERROR);

                const auto & tp = it->second.second.type;
                if (tp->isDateOrDateTime()
                    || (tp->isNullable() && dynamic_cast<const DataTypeNullable *>(tp.get())->getNestedType()->isDateOrDateTime()))
                {
                    Field & field = row[i + 1];
                    if (field.isNull())
                    {
                        it->second.first->insert(row[i + 1]);
                        continue;
                    }
                    UInt64 packed = field.get<UInt64>();
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

                    if (typeid_cast<const DataTypeDateTime *>(tp.get())
                        || (tp->isNullable()
                               && typeid_cast<const DataTypeDateTime *>(
                                      dynamic_cast<const DataTypeNullable *>(tp.get())->getNestedType().get())))
                    {
                        time_t datetime;
                        if (unlikely(year == 0))
                            datetime = 0;
                        else
                        {
                            if (unlikely(month == 0 || day == 0))
                            {
                                throw Exception("wrong datetime format: " + std::to_string(year) + " " + std::to_string(month) + " "
                                        + std::to_string(day) + ".",
                                    ErrorCodes::LOGICAL_ERROR);
                            }
                            datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);
                        }
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

                    // Check overflow for potential un-synced data type widen,
                    // i.e. schema is old and narrow, meanwhile data is new and wide.
                    // So far only integers is possible of overflow.
                    auto nested_tp = tp->isNullable() ? dynamic_cast<const DataTypeNullable *>(tp.get())->getNestedType() : tp;
                    auto & orig_column = *it->second.first;
                    auto & nested_column = it->second.first->isColumnNullable()
                        ? dynamic_cast<ColumnNullable &>(*it->second.first).getNestedColumn()
                        : *it->second.first;
                    auto inserted_index = orig_column.size() - 1;
                    if (!orig_column.isNullAt(inserted_index) && nested_tp->isInteger())
                    {
                        bool overflow = false;
                        if (nested_tp->isUnsignedInteger())
                        {
                            // Unsigned checking by bitwise compare.
                            UInt64 inserted = nested_column.get64(inserted_index);
                            UInt64 orig = row[i + 1].get<UInt64>();
                            overflow = inserted != orig;
                        }
                        else
                        {
                            // Singed checking by arithmetical cast.
                            Int64 inserted = nested_column.getInt(inserted_index);
                            Int64 orig = row[i + 1].get<Int64>();
                            overflow = inserted != orig;
                        }
                        if (overflow)
                        {
                            // Overflow detected, fatal if force_decode is true,
                            // as schema being newer and narrow shouldn't happen.
                            // Otherwise return false to outer, outer should sync schema and try again.
                            if (force_decode)
                                throw Exception(
                                    "Detected overflow for data " + std::to_string(row[i + 1].get<UInt64>()) + " of type " + tp->getName(),
                                    ErrorCodes::LOGICAL_ERROR);

                            return std::make_tuple(block, false);
                        }
                    }
                    // TODO: Consider other kind of type change? I.e. arbitrary type change.
                }
            }
        }
    }

    for (const auto & name : column_names_to_read)
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

    return std::make_tuple(std::move(block), true);
}

} // namespace DB
