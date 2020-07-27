#include <Columns/ColumnsNumber.h>
#include <Core/TMTPKType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/PredecodeTiKVValue.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RowCodec.h>
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

Field GenDefaultField(const ColumnInfo & col_info)
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
            return TiDB::genJsonNull();
        case TiDB::CodecFlagDuration:
            return Field(Int64(0));
        default:
            throw Exception("Not implemented codec flag: " + std::to_string(col_info.getCodecFlag()), ErrorCodes::LOGICAL_ERROR);
    }
}

void ReorderRegionDataReadList(RegionDataReadInfoList & data_list)
{
    // resort the data_list
    // if the order in int64 is like -3 -1 0 1 2 3, the real order in uint64 is 0 1 2 3 -3 -1
    if (data_list.size() > 2)
    {
        bool need_check = false;
        {
            const auto & h1 = std::get<0>(data_list.front());
            const auto & h2 = std::get<0>(data_list.back());
            if ((h1 ^ h2) & SIGN_MASK)
                need_check = true;
        }

        if (need_check)
        {
            auto it = data_list.begin();
            for (; it != data_list.end();)
            {
                const auto & pk = std::get<0>(*it);

                if (pk & SIGN_MASK)
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
    const Timestamp tso,
    RegionScanFilterPtr scan_filter)
{
    ColumnUInt8::Container & delmark_data = delmark_col.getData();
    ColumnUInt64::Container & version_data = version_col.getData();

    delmark_data.reserve(data_list.size());
    version_data.reserve(data_list.size());

    for (const auto & [pk, write_type, commit_ts, value] : data_list)
    {
        std::ignore = value;

        // Ignore data after the start_ts.
        if (commit_ts > tso)
            continue;

        bool should_skip;
        if constexpr (pk_type == TMTPKType::UINT64)
        {
            should_skip = scan_filter != nullptr && scan_filter->filter(static_cast<UInt64>(pk));
        }
        else
        {
            should_skip = scan_filter != nullptr && scan_filter->filter(static_cast<Int64>(pk));
        }
        if (should_skip)
            continue;

        delmark_data.emplace_back(write_type == Region::DelFlag);
        version_data.emplace_back(commit_ts);

        if constexpr (pk_type == TMTPKType::INT64)
            typeid_cast<ColumnVector<Int64> &>(*pk_column).insert(static_cast<Int64>(pk));
        else if constexpr (pk_type == TMTPKType::UINT64)
            typeid_cast<ColumnVector<UInt64> &>(*pk_column).insert(static_cast<UInt64>(pk));
        else
            pk_column->insert(Field(static_cast<Int64>(pk)));
    }
}

std::tuple<Block, bool> readRegionBlock(const TableInfo & table_info,
    const ColumnsDescription & columns,
    const Names & column_names_to_read,
    RegionDataReadInfoList & data_list,
    Timestamp start_ts,
    bool force_decode,
    RegionScanFilterPtr scan_filter)
{
    auto delmark_col = ColumnUInt8::create();
    auto version_col = ColumnUInt64::create();

    /// use map to avoid linear search
    std::unordered_map<String, DataTypePtr> column_type_map;
    for (const auto & p : columns.getAllPhysical())
        column_type_map[p.name] = p.type;

    /// use map to avoid linear search
    std::unordered_map<String, ColumnID> read_column_name_and_ids;
    for (const auto & name : column_names_to_read)
        read_column_name_and_ids[name] = InvalidColumnID;
    if (read_column_name_and_ids.find(MutableSupport::tidb_pk_column_name) != read_column_name_and_ids.end())
        read_column_name_and_ids[MutableSupport::tidb_pk_column_name] = TiDBPkColumnID;


    ColumnID handle_col_id = TiDBPkColumnID;

    constexpr size_t MustHaveColCnt = 3; // pk, del, version

    // column_map contains required columns except del and version.
    ColumnDataInfoMap column_map(column_names_to_read.size() - MustHaveColCnt + 1, EmptyColumnID);

    // visible_column_to_read_lut contains required columns except pk, del and version.
    std::vector<std::pair<ColumnID, size_t>> visible_column_to_read_lut;
    visible_column_to_read_lut.reserve(table_info.columns.size());

    // column_lut contains all columns in the table except pk, del and version.
    ColumnIdToIndex column_lut;
    column_lut.set_empty_key(EmptyColumnID);
    column_lut.set_deleted_key(DeleteColumnID);

    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        auto & column_info = table_info.columns[i];
        ColumnID col_id = column_info.id;
        const String & col_name = column_info.name;
        column_lut.insert({col_id, i});
        if (read_column_name_and_ids.find(col_name) == read_column_name_and_ids.end())
        {
            continue;
        }
        read_column_name_and_ids[col_name] = col_id;

        {
            auto ch_col = NameAndTypePair(col_name, column_type_map[col_name]);
            auto mut_col = ch_col.type->createColumn();
            column_map.insert(col_id, std::move(mut_col), std::move(ch_col), i, data_list.size());
        }

        if (table_info.pk_is_handle && column_info.hasPriKeyFlag())
            handle_col_id = col_id;
        else
            visible_column_to_read_lut.push_back(std::make_pair(col_id, i));
    }

    if (column_names_to_read.size() - MustHaveColCnt != visible_column_to_read_lut.size())
        throw Exception("schema doesn't contain needed columns.", ErrorCodes::LOGICAL_ERROR);

    std::sort(visible_column_to_read_lut.begin(), visible_column_to_read_lut.end());

    if (!table_info.pk_is_handle)
    {
        auto ch_col = NameAndTypePair(MutableSupport::tidb_pk_column_name, MutableSupport::tidb_pk_column_type);
        auto mut_col = ch_col.type->createColumn();
        column_map.insert(handle_col_id, std::move(mut_col), std::move(ch_col), -1, data_list.size());
    }
    else
    {
        // should not contain pk, which may lead to schema not match.
        column_lut.erase(handle_col_id);
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

        func(*delmark_col, *version_col, column_map.getMutableColumnPtr(handle_col_id), data_list, start_ts, scan_filter);
    }

    // optimize for only need handle, tso, delmark.
    if (column_names_to_read.size() > MustHaveColCnt)
    {
        DecodedRecordData decoded_data(visible_column_to_read_lut.size());
        RowPreDecoder pre_decoder{table_info, column_lut};

        // TODO: optimize columns' insertion, use better implementation rather than Field, it's terrible.

        for (const auto & [pk, write_type, commit_ts, value_ptr] : data_list)
        {
            std::ignore = pk;

            // Ignore data after the start_ts.
            if (commit_ts > start_ts)
                continue;

            decoded_data.clear();

            if (write_type == Region::DelFlag)
            {
                for (const auto & item : visible_column_to_read_lut)
                {
                    const auto & column = table_info.columns[item.second];
                    decoded_data.emplace_back(column.id, (column.hasNotNullFlag() ? GenDefaultField(column) : Field()));
                }
            }
            else
            {
                const TiKVValue & value = *value_ptr;
                const DecodedRow * row = value.getDecodedRow().load();
                if (!row)
                {
                    pre_decoder.preDecodeRow(value);
                    row = value.getDecodedRow().load();
                }

                const DecodedFields & decoded_fields = row->decoded_fields;
                const DecodedFields & unknown_fields = row->unknown_fields.fields;

                if (!force_decode)
                {
                    if (row->has_missing_columns || !unknown_fields.empty())
                        return std::make_tuple(Block(), false);
                }

                auto fields_search_it = decoded_fields.begin();
                for (const auto & id_to_idx : visible_column_to_read_lut)
                {
                    if (fields_search_it = std::find_if(fields_search_it,
                            decoded_fields.end(),
                            [&id_to_idx](const DecodedField & e) { return e.col_id >= id_to_idx.first; });
                        fields_search_it != decoded_fields.end() && fields_search_it->col_id == id_to_idx.first)
                    {
                        decoded_data.push_back(fields_search_it++);
                        continue;
                    }

                    const auto & column_info = table_info.columns[id_to_idx.second];

                    if (auto it = findByColumnID(id_to_idx.first, unknown_fields); it != unknown_fields.end())
                    {
                        if (!row->unknown_fields.with_codec_flag)
                        {
                            // If without codec flag, this column is an unknown column in V2 format, re-decode it based on the column info.
                            decoded_data.emplace_back(column_info.id, decodeUnknownColumnV2(it->field, column_info));
                        }
                        else
                        {
                            decoded_data.push_back(it);
                        }
                        continue;
                    }

                    // not null or has no default value, tidb will fill with specific value.
                    decoded_data.emplace_back(column_info.id, column_info.defaultValueToField());
                }
            }

            if (decoded_data.size() != visible_column_to_read_lut.size())
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
            block.insert(
                {std::move(delmark_col), MutableSupport::delmark_column_type, MutableSupport::delmark_column_name, DelMarkColumnID});
        }
        else if (name == MutableSupport::version_column_name)
        {
            block.insert(
                {std::move(version_col), MutableSupport::version_column_type, MutableSupport::version_column_name, VersionColumnID});
        }
        else
        {
            ColumnID col_id = read_column_name_and_ids[name];
            block.insert({std::move(column_map.getMutableColumnPtr(col_id)), column_map.getNameAndTypePair(col_id).type, name, col_id});
        }
    }

    column_map.checkValid();
    return std::make_tuple(std::move(block), true);
}

} // namespace DB
