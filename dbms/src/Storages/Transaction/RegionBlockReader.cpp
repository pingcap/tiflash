#include <Columns/ColumnsNumber.h>
#include <Core/TMTPKType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/RegionBlockReaderHelper.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using TiDB::ColumnInfo;
using TiDB::DatumFlat;
using TiDB::TableInfo;

static Field GenDecodeRow(const ColumnInfo & col_info)
{
    switch (col_info.getCodecFlag())
    {
        case TiDB::CodecFlagNil:
            return Field();
        case TiDB::CodecFlagBytes:
            return Field(String());
        case TiDB::CodecFlagDecimal:
        {
            auto type = createDecimal(col_info.flen, col_info.decimal);
            if (checkDecimal<Decimal32>(*type))
                return Field(DecimalField<Decimal32>(Decimal32(), col_info.decimal));
            else if (checkDecimal<Decimal64>(*type))
                return Field(DecimalField<Decimal64>(Decimal64(), col_info.decimal));
            else if (checkDecimal<Decimal128>(*type))
                return Field(DecimalField<Decimal128>(Decimal128(), col_info.decimal));
            else
                return Field(DecimalField<Decimal256>(Decimal256(), col_info.decimal));
        }
        break;
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
        case TiDB::CodecFlagJson:
            return Field(String());
        case TiDB::CodecFlagDuration:
            return Field(Int64(0));
        default:
            throw Exception("Not implemented codec flag: " + std::to_string(col_info.getCodecFlag()), ErrorCodes::LOGICAL_ERROR);
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

using ColumnIdToInfoIndexMap = google::dense_hash_map<ColumnID, size_t>;
using SchemaAllColumnIds = google::dense_hash_set<ColumnID>;

/// DecodeRowSkip function will try to jump over unnecessary field.
bool DecodeRowSkip(const TiKVValue & value, const ColumnIdToInfoIndexMap & column_id_to_info_index,
    const SchemaAllColumnIds & schema_all_column_ids, DecodedRecordData & decoded_data, const bool force_decode)
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

        if (schema_matches && !schema_all_column_ids.count(col_id))
        {
            schema_matches = false;
            if (!force_decode)
                return schema_matches;
        }

        if (!column_id_to_info_index.count(col_id))
        {
            SkipDatum(cursor, raw_value);
        }
        else
        {
            decoded_data.emplace_back(col_id, DecodeDatum(cursor, raw_value));
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

/// DecodeRow function will try to get pre-decoded fields from value, if is none, just decode its str.
bool DecodeRow(const TiKVValue & value, const ColumnIdToInfoIndexMap & column_id_to_info_index,
    const SchemaAllColumnIds & schema_all_column_ids, DecodedRecordData & decoded_data, const bool force_decode)
{
    auto & decoded_row_info = value.extraInfo();
    const DecodedRow * id_fields_ptr = decoded_row_info.load();
    if (id_fields_ptr)
    {
        bool schema_matches = true;

        const DecodedRow & id_fields = *id_fields_ptr;

        for (auto it = id_fields.cbegin(); it != id_fields.cend(); ++it)
        {
            const auto & ele = *it;
            const auto & col_id = ele.col_id;

            if (schema_matches && !schema_all_column_ids.count(col_id))
            {
                schema_matches = false;
                if (!force_decode)
                    return schema_matches;
            }

            if (column_id_to_info_index.count(col_id))
            {
                decoded_data.push_back(it);
            }
        }

        if (id_fields.size() != schema_all_column_ids.size())
            schema_matches = false;

        return schema_matches;
    }
    else
    {
        return DecodeRowSkip(value, column_id_to_info_index, schema_all_column_ids, decoded_data, force_decode);
    }
}


std::tuple<Block, bool> readRegionBlock(const TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & column_names_to_read,
    RegionDataReadInfoList & data_list,
    Timestamp start_ts,
    bool force_decode)
{
    auto delmark_col = ColumnUInt8::create();
    auto version_col = ColumnUInt64::create();

    ColumnID handle_col_id = TiDBPkColumnID;

    constexpr size_t MustHaveColCnt = 3; // pk, del, version
    constexpr ColumnID EmptyColumnID = TiDBPkColumnID - 1;

    // column_map contains columns in column_names_to_read exclude del and version.
    ColumnDataInfoMap column_map(column_names_to_read.size() - MustHaveColCnt + 1, EmptyColumnID);

    // column_id_to_info_index contains columns in column_names_to_read exclude pk, del and version
    ColumnIdToInfoIndexMap column_id_to_info_index;
    column_id_to_info_index.set_empty_key(EmptyColumnID);

    SchemaAllColumnIds schema_all_column_ids;
    schema_all_column_ids.set_empty_key(EmptyColumnID);

    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        auto & column_info = table_info.columns[i];
        ColumnID col_id = column_info.id;
        const String & col_name = column_info.name;
        schema_all_column_ids.insert(col_id);
        if (std::find(column_names_to_read.begin(), column_names_to_read.end(), col_name) == column_names_to_read.end())
        {
            continue;
        }

        {
            auto ch_col = columns.getPhysical(col_name);
            auto mut_col = ch_col.type->createColumn();
            column_map.insert(col_id, std::move(mut_col), std::move(ch_col), i, data_list.size());
        }

        if (table_info.pk_is_handle && column_info.hasPriKeyFlag())
            handle_col_id = col_id;
        else
            column_id_to_info_index.insert(std::make_pair(col_id, i));
    }

    if (column_names_to_read.size() - MustHaveColCnt != column_id_to_info_index.size())
        throw Exception("schema doesn't contain needed columns.", ErrorCodes::LOGICAL_ERROR);

    if (!table_info.pk_is_handle)
    {
        auto ch_col = columns.getPhysical(MutableSupport::tidb_pk_column_name);
        auto mut_col = ch_col.type->createColumn();
        column_map.insert(handle_col_id, std::move(mut_col), std::move(ch_col), -1, data_list.size());
    }

    const TMTPKType pk_type = getTMTPKType(*column_map.getNameAndTypePair(handle_col_id).type);

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

        func(*delmark_col, *version_col, column_map.getMutableColumnPtr(handle_col_id), data_list, start_ts);
    }

    const size_t target_col_size = column_names_to_read.size() - MustHaveColCnt;

    // optimize for only need handle, tso, delmark.
    if (column_names_to_read.size() > MustHaveColCnt)
    {
        google::dense_hash_set<ColumnID> decoded_col_ids_set;
        decoded_col_ids_set.set_empty_key(EmptyColumnID);
        DecodedRecordData decoded_data(column_id_to_info_index.size());

        // TODO: optimize columns' insertion, use better implementation rather than Field, it's terrible.

        for (const auto & [handle, write_type, commit_ts, value_ptr] : data_list)
        {
            std::ignore = handle;

            // Ignore data after the start_ts.
            if (commit_ts > start_ts)
                continue;

            decoded_data.clear();

            if (write_type == Region::DelFlag)
            {
                for (const auto & item : column_id_to_info_index)
                {
                    const auto & column = table_info.columns[item.second];
                    decoded_data.emplace_back(column.id, GenDecodeRow(column));
                }
            }
            else
            {
                bool schema_matches = DecodeRow(*value_ptr, column_id_to_info_index, schema_all_column_ids, decoded_data, force_decode);
                if (!schema_matches && !force_decode)
                    return std::make_tuple(Block(), false);
            }

            /// Modify `row` by adding missing column values or removing useless column values.
            if (unlikely(decoded_data.size() > column_id_to_info_index.size()))
            {
                throw Exception("read unexpected columns.", ErrorCodes::LOGICAL_ERROR);
            }

            // redundant column values (column id not in current schema) has been dropped when decoding row
            // this branch handles the case when the row doesn't contain all the needed column
            if (decoded_data.size() < column_id_to_info_index.size())
            {
                decoded_col_ids_set.clear_no_resize();
                for (size_t i = 0; i < decoded_data.size(); ++i)
                    decoded_col_ids_set.insert(decoded_data[i].col_id);

                for (const auto & item : column_id_to_info_index)
                {
                    if (decoded_col_ids_set.count(item.first))
                        continue;

                    const auto & column = table_info.columns[item.second];

                    decoded_data.emplace_back(column.id,
                        column.hasNoDefaultValueFlag() ? (column.hasNotNullFlag() ? GenDecodeRow(column) : Field())
                                                       : column.defaultValueToField());
                }
            }

            if (decoded_data.size() != target_col_size)
                throw Exception("decode row error.", ErrorCodes::LOGICAL_ERROR);

            /// Transform `row` to columnar format.
            for (size_t data_idx = 0; data_idx < decoded_data.size(); ++data_idx)
            {
                const ColumnID & col_id = decoded_data[data_idx].col_id;
                const Field & field = decoded_data[data_idx].field;

                auto & col_info = column_map[col_id];
                const ColumnInfo & column_info = table_info.columns[ColumnDataInfoMap::getIndex(col_info)];

                DatumFlat datum(field, column_info.tp);
                const Field & unflattened = datum.field();
                if (datum.overflow(column_info))
                {
                    // Overflow detected, fatal if force_decode is true,
                    // as schema being newer and narrow shouldn't happen.
                    // Otherwise return false to outer, outer should sync schema and try again.
                    if (force_decode)
                    {
                        const auto & data_type = ColumnDataInfoMap::getNameAndTypePair(col_info).type;
                        throw Exception("Detected overflow when decoding data " + std::to_string(unflattened.get<UInt64>()) + " of column "
                                + column_info.name + " with type " + data_type->getName(),
                            ErrorCodes::LOGICAL_ERROR);
                    }

                    return std::make_tuple(Block(), false);
                }
                if (datum.invalidNull(column_info))
                {
                    // Null value with non-null type detected, fatal if force_decode is true,
                    // as schema being newer and with invalid null shouldn't happen.
                    // Otherwise return false to outer, outer should sync schema and try again.
                    if (force_decode)
                    {
                        const auto & data_type = ColumnDataInfoMap::getNameAndTypePair(col_info).type;
                        throw Exception("Detected invalid null when decoding data " + std::to_string(unflattened.get<UInt64>())
                                + " of column " + column_info.name + " with type " + data_type->getName(),
                            ErrorCodes::LOGICAL_ERROR);
                    }

                    return std::make_tuple(Block(), false);
                }
                auto & mut_col = ColumnDataInfoMap::getMutableColumnPtr(col_info);
                mut_col->insert(unflattened);
            }
        }

        decoded_data.checkValid();
    }

    Block block;
    for (const auto & name : column_names_to_read)
    {
        if (name == MutableSupport::delmark_column_name)
        {
            block.insert({std::move(delmark_col), MutableSupport::delmark_column_type, MutableSupport::delmark_column_name});
        }
        else if (name == MutableSupport::version_column_name)
        {
            block.insert({std::move(version_col), MutableSupport::version_column_type, MutableSupport::version_column_name});
        }
        else
        {
            ColumnID col_id = table_info.getColumnID(name);
            block.insert({std::move(column_map.getMutableColumnPtr(col_id)), column_map.getNameAndTypePair(col_id).type, name});
        }
    }

    column_map.checkValid();
    return std::make_tuple(std::move(block), true);
}

} // namespace DB
