#include <Columns/ColumnsNumber.h>
#include <Core/TMTPKType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/DatumCodec.h>
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
            if ((h1 & SIGN_MASK) && !(h2 & SIGN_MASK))
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
    std::vector<ColumnID> & pk_column_ids,
    ColumnDataInfoMap & column_map,
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

        bool should_skip = false;
        if constexpr (pk_type != TMTPKType::STRING)
        {
            if constexpr (pk_type == TMTPKType::UINT64)
            {
                should_skip = scan_filter != nullptr && scan_filter->filter(static_cast<UInt64>(pk));
            }
            else
            {
                should_skip = scan_filter != nullptr && scan_filter->filter(static_cast<Int64>(pk));
            }
        }
        if (should_skip)
            continue;

        delmark_data.emplace_back(write_type == Region::DelFlag);
        version_data.emplace_back(commit_ts);

        if constexpr (pk_type == TMTPKType::INT64)
            typeid_cast<ColumnVector<Int64> &>(*(column_map.getMutableColumnPtr(pk_column_ids[0]))).insert(static_cast<Int64>(pk));
        else if constexpr (pk_type == TMTPKType::UINT64)
            typeid_cast<ColumnVector<UInt64> &>(*(column_map.getMutableColumnPtr(pk_column_ids[0]))).insert(static_cast<UInt64>(pk));
        else if constexpr (pk_type == TMTPKType::STRING)
        {
            column_map.getMutableColumnPtr(pk_column_ids[0])->insert(Field(pk->data(), pk->size()));
            /// decode key and insert other pk columns if needed
            size_t cursor = 0, pos = 0;
            while (cursor < pk->size() && pk_column_ids.size() > pos + 1)
            {
                Field value = DecodeDatum(cursor, *pk);
                if (pk_column_ids[pos + 1] != EmptyColumnID)
                    column_map.getMutableColumnPtr(pk_column_ids[pos + 1])->insert(value);
                pos++;
            }
        }
        else
            column_map.getMutableColumnPtr(pk_column_ids[0])->insert(Field(static_cast<Int64>(pk)));
    }
}

template <TMTPKType pk_type>
bool setColumnValues(ColumnUInt8 & delmark_col,
    ColumnUInt64 & version_col,
    std::vector<ColumnID> & pk_column_ids,
    const std::vector<std::pair<ColumnID, size_t>> & visible_column_to_read_lut,
    ColumnIdToIndex & column_lut,
    ColumnDataInfoMap & column_map,
    const RegionDataReadInfoList & data_list,
    const Timestamp tso,
    bool need_decode_value,
    const TableInfo & table_info,
    bool force_decode,
    RegionScanFilterPtr scan_filter)
{
    ColumnUInt8::Container & delmark_data = delmark_col.getData();
    ColumnUInt64::Container & version_data = version_col.getData();

    delmark_data.reserve(data_list.size());
    version_data.reserve(data_list.size());

    DecodedRecordData decoded_data(visible_column_to_read_lut.size());
    std::unique_ptr<const DecodedRow> tmp_row; // decode row into Field list here for temporary use if necessary.
    size_t index = 0;
    for (const auto & [pk, write_type, commit_ts, value_ptr] : data_list)
    {
        // Ignore data after the start_ts.
        if (commit_ts > tso)
            continue;

        bool should_skip = false;
        if constexpr (pk_type != TMTPKType::STRING)
        {
            if constexpr (pk_type == TMTPKType::UINT64)
            {
                should_skip = scan_filter != nullptr && scan_filter->filter(static_cast<UInt64>(pk));
            }
            else
            {
                should_skip = scan_filter != nullptr && scan_filter->filter(static_cast<Int64>(pk));
            }
        }
        if (should_skip)
            continue;

        /// set delmark and version column
        delmark_data.emplace_back(write_type == Region::DelFlag);
        version_data.emplace_back(commit_ts);

        /// Decode value, all the columns except the pk column should be encoded in the value
        /// For pk column, if is_common_handle = true or pk_is_handle = true, the pk column might
        /// be only encoded in the key, if a column exists both in value and key, use the one in
        /// the value(Based on TiDB's new design, maybe in the future, if a column exists both in
        /// the key and value, we need to combine them and generate the final column field)
        if (need_decode_value)
        {
            decoded_data.clear();
            size_t skipped_pk_columns = 0;
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
                const DecodedRow * row = nullptr;
                {
                    // not like old logic, do not store Field cache with value in order to reduce memory cost.
                    tmp_row.reset(decodeRow(value.getStr(), table_info, column_lut));
                    row = tmp_row.get();
                }

                const DecodedFields & decoded_fields = row->decoded_fields;
                const DecodedFields & unknown_fields = row->unknown_fields.fields;

                if (!force_decode)
                {
                    if (row->has_missing_columns || !unknown_fields.empty())
                        return false;
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
                    // if the table is clustered index, the primary column can be derived from the key if it is not in the value
                    if (!(table_info.is_common_handle && column_info.hasPriKeyFlag()))
                        decoded_data.emplace_back(column_info.id, column_info.defaultValueToField());
                    else
                        skipped_pk_columns++;
                }
            }

            if (decoded_data.size() + skipped_pk_columns != visible_column_to_read_lut.size())
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

                    return false;
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

                    return false;
                }
                auto & mut_col = ColumnDataInfoMap::getMutableColumnPtr(col_info);
                mut_col->insert(unflattened);
            }
        }

        if constexpr (pk_type == TMTPKType::INT64)
            typeid_cast<ColumnVector<Int64> &>(*(column_map.getMutableColumnPtr(pk_column_ids[0]))).insert(static_cast<Int64>(pk));
        else if constexpr (pk_type == TMTPKType::UINT64)
            typeid_cast<ColumnVector<UInt64> &>(*(column_map.getMutableColumnPtr(pk_column_ids[0]))).insert(static_cast<UInt64>(pk));
        else if constexpr (pk_type == TMTPKType::STRING)
        {
            column_map.getMutableColumnPtr(pk_column_ids[0])->insert(Field(pk->data(), pk->size()));
            /// decode key and insert pk columns if needed
            size_t cursor = 0, pos = 0;
            while (cursor < pk->size() && pk_column_ids.size() > pos + 1)
            {
                Field value = DecodeDatum(cursor, *pk);
                /// for a pk col, if it does not exist in the value, then decode it from the key
                if (pk_column_ids[pos + 1] != EmptyColumnID && column_map.getMutableColumnPtr(pk_column_ids[pos + 1])->size() == index)
                    column_map.getMutableColumnPtr(pk_column_ids[pos + 1])->insert(value);
                pos++;
            }
        }
        else
        {
            // The pk_type must be Int32/Uint32 or more narrow type
            // so cannot tell its' exact type here, just use `insert(Field)`
            HandleID handle_value(static_cast<Int64>(pk));
            auto & pk_column = column_map.getMutableColumnPtr(pk_column_ids[0]);
            pk_column->insert(Field(handle_value));
            if (unlikely(pk_column->getInt(index) != handle_value))
            {
                if (!force_decode)
                {
                    return false;
                }
                else
                {
                    throw Exception("Detected overflow value when decoding pk column of type " + pk_column->getName(),
                                    ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
        index++;
    }
    decoded_data.checkValid();
    return true;
}

RegionBlockReader::RegionBlockReader(const ManageableStoragePtr & storage)
    : RegionBlockReader(storage->getTableInfo(), storage->getColumns())
{
    // For delta-tree, we don't need to reorder for uint64_pk
    do_reorder_for_uint64_pk = (storage->engineType() != TiDB::StorageEngine::DT);
}

RegionBlockReader::RegionBlockReader(const TiDB::TableInfo & table_info_, const ColumnsDescription & columns_)
    : table_info(table_info_), columns(columns_), scan_filter(nullptr)
{}

std::tuple<Block, bool> RegionBlockReader::read(const Names & column_names_to_read, RegionDataReadInfoList & data_list, bool force_decode)
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
    /// column_id => NameAndType/MutableColumnPtr
    ColumnDataInfoMap column_map(column_names_to_read.size() - MustHaveColCnt + 1, EmptyColumnID);

    // visible_column_to_read_lut contains required columns except pk, del and version.
    std::vector<std::pair<ColumnID, size_t>> visible_column_to_read_lut;
    visible_column_to_read_lut.reserve(table_info.columns.size());

    // column_lut contains all columns in the table except pk, del and version.
    /// column_id => column pos in table_info
    ColumnIdToIndex column_lut;
    column_lut.set_empty_key(EmptyColumnID);
    column_lut.set_deleted_key(DeleteColumnID);

    std::vector<ColumnID> readed_primary_key_column_ids;
    /// column name => primary key offset
    std::unordered_map<String, size_t> primary_key_column_pos_map;
    if (table_info.is_common_handle)
    {
        auto & primary_index_info = table_info.getPrimaryIndexInfo();
        readed_primary_key_column_ids.resize(primary_index_info.idx_cols.size(), EmptyColumnID);
        for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
        {
            const auto & col = primary_index_info.idx_cols[i];
            primary_key_column_pos_map[col.name] = i;
        }
    }

    for (size_t i = 0; i < table_info.columns.size(); i++)
    {
        auto & column_info = table_info.columns[i];
        ColumnID col_id = column_info.id;
        const String & col_name = column_info.name;
        if (!(table_info.pk_is_handle && column_info.hasPriKeyFlag()))
        {
            column_lut.insert({col_id, i});
        }
        if (read_column_name_and_ids.find(col_name) == read_column_name_and_ids.end())
        {
            continue;
        }
        read_column_name_and_ids[col_name] = col_id;
        const auto & it = primary_key_column_pos_map.find(col_name);
        if (it != primary_key_column_pos_map.end())
        {
            readed_primary_key_column_ids[it->second] = col_id;
        }

        {
            auto ch_col = NameAndTypePair(col_name, column_type_map[col_name]);
            auto mut_col = ch_col.type->createColumn();
            column_map.insert(col_id, std::move(mut_col), std::move(ch_col), i, data_list.size());
        }

        if (table_info.pk_is_handle && column_info.hasPriKeyFlag())
            handle_col_id = col_id;
        else
            visible_column_to_read_lut.emplace_back(col_id, i);
    }

    if (column_names_to_read.size() - MustHaveColCnt != visible_column_to_read_lut.size())
        throw Exception("schema doesn't contain needed columns.", ErrorCodes::LOGICAL_ERROR);

    std::sort(visible_column_to_read_lut.begin(), visible_column_to_read_lut.end());

    if (!table_info.pk_is_handle)
    {
        auto ch_col = NameAndTypePair(MutableSupport::tidb_pk_column_name,
            table_info.is_common_handle ? MutableSupport::tidb_pk_column_string_type : MutableSupport::tidb_pk_column_int_type);
        auto mut_col = ch_col.type->createColumn();
        column_map.insert(handle_col_id, std::move(mut_col), std::move(ch_col), -1, data_list.size());
    }

    const TMTPKType pk_type = getTMTPKType(*column_map.getNameAndTypePair(handle_col_id).type);

    if (do_reorder_for_uint64_pk && pk_type == TMTPKType::UINT64)
        ReorderRegionDataReadList(data_list);

    {
        auto func = setColumnValues<TMTPKType::UNSPECIFIED>;

        switch (pk_type)
        {
            case TMTPKType::INT64:
                func = setColumnValues<TMTPKType::INT64>;
                break;
            case TMTPKType::UINT64:
                func = setColumnValues<TMTPKType::UINT64>;
                break;
            case TMTPKType::STRING:
                func = setColumnValues<TMTPKType::STRING>;
                break;
            default:
                break;
        }

        std::vector<ColumnID> pk_column_ids;
        pk_column_ids.emplace_back(handle_col_id);
        if (table_info.is_common_handle)
        {
            for (size_t i = 0; i < readed_primary_key_column_ids.size(); i++)
            {
                pk_column_ids.emplace_back(readed_primary_key_column_ids[i]);
            }
        }
        if (!func(*delmark_col, *version_col, pk_column_ids, visible_column_to_read_lut, column_lut, column_map, data_list, start_ts,
                column_names_to_read.size() > MustHaveColCnt, table_info, force_decode, scan_filter))
            return std::make_tuple<Block, bool>({}, false);
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
