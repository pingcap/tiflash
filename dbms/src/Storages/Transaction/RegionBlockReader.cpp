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
#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

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
            if ((h1 ^ h2) & SIGN_MASK)
                need_check = true;
        }

        if (need_check)
        {
            auto it = data_list.begin();
            for (; it != data_list.end();)
            {
                const auto handle = std::get<0>(*it);

                if (handle & SIGN_MASK)
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

inline bool DecodeRowSkip(const TiKVValue & value, const google::dense_hash_set<ColumnID> & column_ids_to_read,
    std::vector<ColumnID> & col_ids, std::vector<Field> & fields, const google::dense_hash_set<ColumnID> & schema_all_column_ids)
{
    const String & raw_value = value.getStr();
    size_t cursor = 0;
    bool schema_matches = true;
    size_t column_cnt = 0;
    while (cursor < raw_value.size())
    {
        Field f = DecodeDatum(cursor, raw_value);
        if (f.isNull())
            break;

        ColumnID col_id = f.get<ColumnID>();
        column_cnt++;
        if (!schema_all_column_ids.count(col_id))
        {
            schema_matches = false;
        }
        if (!column_ids_to_read.count(col_id))
        {
            SkipDatum(cursor, raw_value);
        }
        else
        {
            col_ids.push_back(col_id);
            fields.emplace_back(DecodeDatum(cursor, raw_value));
        }
    }
    if (column_cnt != schema_all_column_ids.size())
    {
        schema_matches = false;
    }

    if (cursor != raw_value.size())
        throw Exception("DecodeRow cursor is not end", ErrorCodes::LOGICAL_ERROR);
    return schema_matches;
}

inline bool DecodeRow(const TiKVValue & value, const google::dense_hash_set<ColumnID> & column_ids_to_read, std::vector<ColumnID> & col_ids,
    std::vector<Field> & fields, const google::dense_hash_set<ColumnID> & schema_all_column_ids)
{
    auto & decoded_row_info = value.extraInfo();
    const DecodedRow * id_fields_ptr = decoded_row_info.load();
    if (id_fields_ptr)
    {
        bool schema_matches = true;

        const DecodedRow & id_fields = *id_fields_ptr;
        for (const auto & ele : id_fields)
        {
            const auto & col_id = ele.col_id;
            if (!schema_all_column_ids.count(col_id))
            {
                schema_matches = false;
            }
            if (column_ids_to_read.count(col_id))
            {
                col_ids.push_back(col_id);
                fields.push_back(ele.field);
            }
        }
        return schema_matches;
    }
    else
    {
        return DecodeRowSkip(value, column_ids_to_read, col_ids, fields, schema_all_column_ids);
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

    constexpr ColumnID EmptyColumnID = InvalidColumnID - 1;

    using ColTypePair = std::pair<MutableColumnPtr, NameAndTypePair>;
    google::dense_hash_map<ColumnID, std::shared_ptr<ColTypePair>> column_map;
    column_map.set_empty_key(EmptyColumnID);

    google::dense_hash_map<ColumnID, size_t> column_id_to_info_index_map;
    column_id_to_info_index_map.set_empty_key(EmptyColumnID);

    google::dense_hash_set<ColumnID> column_ids_to_read;
    column_ids_to_read.set_empty_key(EmptyColumnID);

    google::dense_hash_set<ColumnID> schema_all_column_ids;
    schema_all_column_ids.set_empty_key(EmptyColumnID);

    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        auto & column_info = table_info.columns[i];
        ColumnID col_id = column_info.id;
        String col_name = column_info.name;
        schema_all_column_ids.insert(col_id);
        if (std::find(column_names_to_read.begin(), column_names_to_read.end(), col_name) == column_names_to_read.end())
        {
            continue;
        }
        auto ch_col = columns.getPhysical(col_name);
        column_map.insert(std::make_pair(col_id, std::make_shared<ColTypePair>(ch_col.type->createColumn(), ch_col)));
        column_map[col_id]->first->reserve(data_list.size());
        if (table_info.pk_is_handle && column_info.hasPriKeyFlag())
            handle_col_id = col_id;
        else
        {
            column_ids_to_read.insert(col_id);
            column_id_to_info_index_map.insert(std::make_pair(col_id, i));
        }
    }
    if (column_names_to_read.size() - 3 != column_ids_to_read.size())
        throw Exception("schema doesn't contain needed columns.", ErrorCodes::LOGICAL_ERROR);

    if (!table_info.pk_is_handle)
    {
        auto ch_col = columns.getPhysical(MutableSupport::tidb_pk_column_name);
        column_map.insert(std::make_pair(handle_col_id, std::make_shared<ColTypePair>(ch_col.type->createColumn(), ch_col)));
        column_map[handle_col_id]->first->reserve(data_list.size());
    }

    const TMTPKType pk_type = getTMTPKType(*column_map[handle_col_id]->second.type);

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

        func(*delmark_col, *version_col, column_map[handle_col_id]->first, data_list, start_ts);
    }

    const auto & date_lut = DateLUT::instance();

    const size_t target_col_size = column_names_to_read.size() - 3;

    Block block;

    // optimize for only need handle, tso, delmark.
    if (column_names_to_read.size() > 3)
    {
        google::dense_hash_set<ColumnID> decoded_col_ids_set;
        decoded_col_ids_set.set_empty_key(EmptyColumnID);

        // TODO: optimize columns' insertion, use better implementation rather than Field, it's terrible.
        std::vector<ColumnID> decoded_col_ids;
        std::vector<Field> decoded_fields;
        decoded_col_ids.reserve(target_col_size);
        decoded_fields.reserve(target_col_size);

        for (const auto & [handle, write_type, commit_ts, value_ptr] : data_list)
        {
            std::ignore = handle;

            // Ignore data after the start_ts.
            if (commit_ts > start_ts)
                continue;

            decoded_col_ids.clear();
            decoded_fields.clear();
            if (write_type == Region::DelFlag)
            {
                for (auto col_id : column_ids_to_read)
                {
                    const auto & column = table_info.columns[column_id_to_info_index_map[col_id]];

                    decoded_col_ids.push_back(column.id);
                    decoded_fields.emplace_back(GenDecodeRow(column.getCodecFlag()));
                }
            }
            else
            {
                bool schema_matches = DecodeRow(*value_ptr, column_ids_to_read, decoded_col_ids, decoded_fields, schema_all_column_ids);
                if (!schema_matches && !force_decode)
                {
                    return std::make_tuple(block, false);
                }
                if (decoded_col_ids.empty())
                {
                    // all field is null
                    decoded_fields.clear();
                }
            }

            if (decoded_col_ids.size() != decoded_fields.size())
                throw Exception("row size is wrong.", ErrorCodes::LOGICAL_ERROR);

            /// Modify `row` by adding missing column values or removing useless column values.
            if (unlikely(decoded_col_ids.size() > column_ids_to_read.size()))
            {
                throw Exception("read unexpected columns.", ErrorCodes::LOGICAL_ERROR);
            }

            // redundant column values (column id not in current schema) has been dropped when decoding row
            // this branch handles the case when the row doesn't contain all the needed column
            if (decoded_col_ids.size() < column_ids_to_read.size())
            {
                decoded_col_ids_set.clear();
                decoded_col_ids_set.insert(decoded_col_ids.begin(), decoded_col_ids.end());

                for (auto col_id : column_ids_to_read)
                {
                    if (decoded_col_ids_set.count(col_id))
                        continue;

                    const auto & column = table_info.columns[column_id_to_info_index_map[col_id]];
                    decoded_col_ids.push_back(column.id);
                    if (column.hasNoDefaultValueFlag())
                        // Fill `zero` value if NOT NULL specified or else NULL.
                        decoded_fields.push_back(column.hasNotNullFlag() ? GenDecodeRow(column.getCodecFlag()) : Field());
                    else
                        // Fill default value.
                        decoded_fields.push_back(column.defaultValueToField());
                }
            }

            if (decoded_col_ids.size() != target_col_size || decoded_fields.size() != target_col_size)
                throw Exception("decode row error.", ErrorCodes::LOGICAL_ERROR);

            /// Transform `row` to columnar format.
            for (size_t i = 0; i < decoded_col_ids.size(); i++)
            {
                ColumnID col_id = decoded_col_ids[i];
                auto it = column_map.find(col_id);
                if (it == column_map.end())
                    throw Exception("col_id not found in column_map", ErrorCodes::LOGICAL_ERROR);

                auto & name_type = it->second->second;
                auto & mut_col = it->second->first;
                const auto & tp = name_type.type;
                if (tp->isDateOrDateTime()
                    || (tp->isNullable() && dynamic_cast<const DataTypeNullable *>(tp.get())->getNestedType()->isDateOrDateTime()))
                {
                    Field & field = decoded_fields[i];
                    if (field.isNull())
                    {
                        mut_col->insert(decoded_fields[i]);
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
                        mut_col->insert(static_cast<Int64>(datetime));
                    }
                    else
                    {
                        auto date = date_lut.makeDayNum(year, month, day);
                        Field date_field(static_cast<Int64>(date));
                        mut_col->insert(date_field);
                    }
                }
                else
                {
                    mut_col->insert(decoded_fields[i]);

                    // Check overflow for potential un-synced data type widen,
                    // i.e. schema is old and narrow, meanwhile data is new and wide.
                    // So far only integers is possible of overflow.
                    auto & nested_tp = tp->isNullable() ? dynamic_cast<const DataTypeNullable *>(tp.get())->getNestedType() : tp;
                    auto & orig_column = *mut_col;
                    auto & nested_column
                        = mut_col->isColumnNullable() ? dynamic_cast<ColumnNullable &>(*mut_col).getNestedColumn() : *mut_col;
                    auto inserted_index = orig_column.size() - 1;
                    if (!orig_column.isNullAt(inserted_index) && nested_tp->isInteger())
                    {
                        bool overflow = false;
                        if (nested_tp->isUnsignedInteger())
                        {
                            // Unsigned checking by bitwise compare.
                            UInt64 inserted = nested_column.get64(inserted_index);
                            UInt64 orig = decoded_fields[i].get<UInt64>();
                            overflow = inserted != orig;
                        }
                        else
                        {
                            // Singed checking by arithmetical cast.
                            Int64 inserted = nested_column.getInt(inserted_index);
                            Int64 orig = decoded_fields[i].get<Int64>();
                            overflow = inserted != orig;
                        }
                        if (overflow)
                        {
                            // Overflow detected, fatal if force_decode is true,
                            // as schema being newer and narrow shouldn't happen.
                            // Otherwise return false to outer, outer should sync schema and try again.
                            if (force_decode)
                                throw Exception("Detected overflow for data " + std::to_string(decoded_fields[i].get<UInt64>())
                                        + " of type " + tp->getName(),
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
            block.insert({std::move(column_map[col_id]->first), column_map[col_id]->second.type, name});
        }
    }

    return std::make_tuple(std::move(block), true);
}

} // namespace DB
