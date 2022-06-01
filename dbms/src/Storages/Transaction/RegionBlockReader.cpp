// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/ColumnsNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionBlockReader.h>
#include <Storages/Transaction/RowCodec.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

RegionBlockReader::RegionBlockReader(DecodingStorageSchemaSnapshotConstPtr schema_snapshot_)
    : schema_snapshot{std::move(schema_snapshot_)}
{}

bool RegionBlockReader::read(Block & block, const RegionDataReadInfoList & data_list, bool force_decode)
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
bool RegionBlockReader::readImpl(Block & block, const RegionDataReadInfoList & data_list, bool force_decode)
{
    if (unlikely(block.columns() != schema_snapshot->column_defines->size()))
        throw Exception("block structure doesn't match schema_snapshot.", ErrorCodes::LOGICAL_ERROR);

    const auto & read_column_ids = schema_snapshot->sorted_column_id_with_pos;
    const auto & pk_column_ids = schema_snapshot->pk_column_ids;
    const auto & pk_pos_map = schema_snapshot->pk_pos_map;

    auto column_ids_iter = read_column_ids.begin();
    size_t next_column_pos = 0;

    /// every table in tiflash must have an extra handle column, it either
    ///   1. sync from tidb (when the table doesn't have a primary key of int kind type and cluster index is not enabled)
    ///   2. copy (and cast if need) from the pk column (when the table have a primary key of int kind type)
    ///   3. encoded from the pk columns (when the table doesn't have a primary key of int kind type when cluster index is enabled)
    ///
    /// extra handle, del, version column is with column id smaller than other visible column id,
    /// so they must exists before all other columns, and we can get them before decoding other columns
    ColumnUInt8 * raw_delmark_col = nullptr;
    ColumnUInt64 * raw_version_col = nullptr;
    const size_t invalid_column_pos = std::numeric_limits<size_t>::max();
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
    // extra handle, del, version must exists
    constexpr size_t MustHaveColCnt = 3; // NOLINT(readability-identifier-naming)
    if (unlikely(next_column_pos != MustHaveColCnt))
        throw Exception("del, version column must exist before all other visible columns.", ErrorCodes::LOGICAL_ERROR);

    ColumnUInt8::Container & delmark_data = raw_delmark_col->getData();
    ColumnUInt64::Container & version_data = raw_version_col->getData();
    delmark_data.reserve(data_list.size());
    version_data.reserve(data_list.size());
    bool need_decode_value = block.columns() > MustHaveColCnt;
    if (need_decode_value)
    {
        size_t expected_rows = data_list.size();
        for (size_t pos = next_column_pos; pos < block.columns(); pos++)
        {
            auto * raw_column = const_cast<IColumn *>((block.getByPosition(pos)).column.get());
            raw_column->reserve(expected_rows);
        }
    }
    size_t index = 0;
    for (const auto & [pk, write_type, commit_ts, value_ptr] : data_list)
    {
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
                    const auto & ci = schema_snapshot->column_infos[column_ids_iter_copy->second];
                    // when pk is handle, we can decode the pk from the key
                    if (!(schema_snapshot->pk_is_handle && ci.hasPriKeyFlag()))
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
                if (schema_snapshot->pk_is_handle)
                {
                    if (!appendRowToBlock(*value_ptr, column_ids_iter, read_column_ids.end(), block, next_column_pos, schema_snapshot->column_infos, schema_snapshot->pk_column_ids[0], force_decode))
                        return false;
                }
                else
                {
                    if (!appendRowToBlock(*value_ptr, column_ids_iter, read_column_ids.end(), block, next_column_pos, schema_snapshot->column_infos, InvalidColumnID, force_decode))
                        return false;
                }
            }
        }

        /// set extra handle column and pk columns if need
        if constexpr (pk_type != TMTPKType::STRING)
        {
            // extra handle column's type is always Int64
            auto * raw_extra_column = const_cast<IColumn *>((block.getByPosition(extra_handle_column_pos)).column.get());
            static_cast<ColumnInt64 *>(raw_extra_column)->getData().push_back(Int64(pk));
            if (!pk_column_ids.empty())
            {
                auto * raw_pk_column = const_cast<IColumn *>((block.getByPosition(pk_pos_map.at(pk_column_ids[0]))).column.get());
                if constexpr (pk_type == TMTPKType::INT64)
                    static_cast<ColumnInt64 *>(raw_pk_column)->getData().push_back(Int64(pk));
                else if constexpr (pk_type == TMTPKType::UINT64)
                    static_cast<ColumnUInt64 *>(raw_pk_column)->getData().push_back(UInt64(pk));
                else
                {
                    // The pk_type must be Int32/Uint32 or more narrow type
                    // so cannot tell its' exact type here, just use `insert(Field)`
                    auto handle_value(static_cast<Int64>(pk));
                    raw_pk_column->insert(Field(handle_value));
                    if (unlikely(raw_pk_column->getInt(index) != handle_value))
                    {
                        if (!force_decode)
                        {
                            return false;
                        }
                        else
                        {
                            throw Exception("Detected overflow value when decoding pk column of type " + raw_pk_column->getName(),
                                            ErrorCodes::LOGICAL_ERROR);
                        }
                    }
                }
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
                /// for a pk col, if it does not exist in the value, then decode it from the key
                /// some examples that we must decode column value from value part
                ///   1) if collation is enabled, the extra key may be a transformation of the original value of pk cols
                ///   2) the primary key may just be a prefix of a column
                auto * raw_pk_column = const_cast<IColumn *>(block.getByPosition(pk_pos_map.at(pk_column_ids[pos])).column.get());
                if (raw_pk_column->size() == index)
                {
                    raw_pk_column->insert(value);
                }
                pos++;
            }
        }
        index++;
    }
    return true;
}

} // namespace DB
