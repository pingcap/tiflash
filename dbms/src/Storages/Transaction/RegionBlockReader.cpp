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

RegionBlockReader::RegionBlockReader(DecodingStorageSchemaSnapshotConstPtr schema_snapshot_)
    : schema_snapshot{std::move(schema_snapshot_)}
{}

bool RegionBlockReader::read(Block & block, RegionDataReadInfoList & data_list, bool force_decode)
{
    switch (schema_snapshot->pk_type)
    {
    case TMTPKType::INT64:
        return readImpl<TMTPKType::INT64>(block, data_list, force_decode);
    case TMTPKType::UINT64:
        return readImpl<TMTPKType::UINT64>(block, data_list, force_decode);
    case TMTPKType::STRING:
        return readImpl<TMTPKType::STRING>(block, data_list, force_decode);
    default:
        return readImpl<TMTPKType::UNSPECIFIED>(block, data_list, force_decode);
    }
}

template <TMTPKType pk_type>
bool RegionBlockReader::readImpl(Block & block, RegionDataReadInfoList & data_list, bool force_decode)
{
    const auto & read_column_ids = schema_snapshot->sorted_column_id_with_pos;
    const auto & pk_column_ids = schema_snapshot->pk_column_ids;
    const auto & pk_pos_map = schema_snapshot->pk_pos_map;

    SortedColumnIDWithPosConstIter column_ids_iter = read_column_ids.begin();
    size_t next_column_pos = 0;

    /// every table in tiflash must have an extra handle column, it either
    ///   1. sync from tidb (when the table doesn't have a primary key of int kind type and cluster index is not enabled)
    ///   2. copy (and cast if need) from the pk column (when the table have a primary key of int kind type)
    ///   3. encoded from the pk columns (when the table doesn't have a primary key of int kind type when cluster index is enabled)
    ///
    /// extra handle, del, version column is with column id smaller than other visible column id,
    /// so they must exists before all other columns, and we can get them before decoding other columns
    ColumnUInt8 *raw_delmark_col = nullptr;
    ColumnUInt64 *raw_version_col = nullptr;
    const size_t invalid_column_pos = reinterpret_cast<size_t>(std::numeric_limits<size_t>::max);
    // we cannot figure out extra_handle's column type now, so we just remember it's pos here
    size_t extra_handle_column_pos = invalid_column_pos;
    while (raw_delmark_col == nullptr || raw_version_col == nullptr || extra_handle_column_pos == invalid_column_pos)
    {
        if (column_ids_iter->first == DelMarkColumnID)
        {
            raw_delmark_col = static_cast<ColumnUInt8 *>(const_cast<IColumn *>(block.getByPosition(next_column_pos).column.get()));
        }
        else if (column_ids_iter->first == VersionColumnID)
        {
            raw_version_col = static_cast<ColumnUInt64 *>(const_cast<IColumn *>(block.getByPosition(next_column_pos).column.get()));
        }
        else if (column_ids_iter->first == TiDBPkColumnID)
        {
            extra_handle_column_pos = next_column_pos;
        }
        next_column_pos++;
        column_ids_iter++;
    }
    constexpr size_t MustHaveColCnt = 3; // extra handle, del, version
    if (unlikely(next_column_pos != MustHaveColCnt))
        throw Exception("del, version column must exist before all other visible columns.", ErrorCodes::LOGICAL_ERROR);

    ColumnUInt8::Container & delmark_data = raw_delmark_col->getData();
    ColumnUInt64::Container & version_data = raw_version_col->getData();
    delmark_data.reserve(data_list.size());
    version_data.reserve(data_list.size());
    bool need_decode_value = block.columns() > MustHaveColCnt;
    size_t index = 0;
    // TODO: reserve data_list.size() in every column
    for (const auto & [pk, write_type, commit_ts, value_ptr] : data_list)
    {
        // Ignore data after the start_ts.
        if (commit_ts > start_ts)
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

        if (need_decode_value)
        {
            if (write_type == Region::DelFlag)
            {
                auto column_ids_iter_copy = column_ids_iter;
                auto next_column_pos_copy = next_column_pos;
                while (column_ids_iter_copy != read_column_ids.end())
                {
                    auto & cd = (*schema_snapshot->column_defines)[column_ids_iter_copy->second];
                    // !pk_pos_map.empty() means the table is common index, or pk is handle, so we can decode the pk from the key
                    if (pk_pos_map.empty() || !cd.is_pk)
                    {
                        auto * raw_column = const_cast<IColumn *>((block.getByPosition(next_column_pos_copy)).column.get());
                        raw_column->insertDefault();
                    }
                    column_ids_iter_copy++;
                    next_column_pos_copy++;
                }
            }
            else
            {
                if (!appendRowToBlock(*value_ptr, column_ids_iter, read_column_ids.end(), block, next_column_pos, schema_snapshot->column_defines, force_decode))
                    return false;
            }
        }

        /// set extra handle column and pk columns if need
        if constexpr (pk_type != TMTPKType::STRING)
        {
            // extra handle column's type is always Int64
            auto * raw_extra_column = const_cast<IColumn *>((block.getByPosition(extra_handle_column_pos)).column.get());
            raw_extra_column->insert(Field(static_cast<Int64>(pk)));
            if (!pk_column_ids.empty())
            {
                auto * raw_pk_column = const_cast<IColumn *>((block.getByPosition(pk_pos_map.at(pk_column_ids[0]))).column.get());
                if constexpr (pk_type == TMTPKType::INT64)
                    raw_pk_column->insert(static_cast<Int64>(pk));
                else if constexpr (pk_type == TMTPKType::UINT64)
                    raw_pk_column->insert(static_cast<UInt64>(pk));
                else
                    raw_pk_column->insert(static_cast<Int64>(pk));
            }
        }
        else
        {
            auto * raw_extra_column = const_cast<IColumn *>((block.getByPosition(extra_handle_column_pos)).column.get());
            raw_extra_column->insertData(pk->data(), pk->size());
            /// decode key and insert pk columns if needed
            size_t cursor = 0, pos = 0;
            while (cursor < pk->size() && pos < pk_column_ids.size())
            {
                Field value = DecodeDatum(cursor, *pk);
                if (pk_pos_map.find(pk_column_ids[pos]) != pk_pos_map.end())
                {
                    /// for a pk col, if it does not exist in the value, then decode it from the key
                    auto * raw_pk_column = const_cast<IColumn *>(block.getByPosition(pk_pos_map.at(pk_column_ids[pos])).column.get());
                    if (raw_pk_column->size() == index)
                        raw_pk_column->insert(value);
                }
                pos++;
            }
        }
        index++;
    }
    // TODO: remove it
    size_t expected_rows = block.rows();
    for (size_t i = 0; i < block.columns(); i++)
    {
        auto & ch_column = block.getByPosition(i);
        if (ch_column.column->size() != expected_rows)
            throw Exception("column " + ch_column.name + " rows " + std::to_string(ch_column.column->size()) + " but block rows " + std::to_string(expected_rows), ErrorCodes::LOGICAL_ERROR);
    }
    return true;
}

} // namespace DB
