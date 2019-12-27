#pragma once

#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVDecodedValue.h>
#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

namespace DB
{

using ColumnIds = google::dense_hash_set<ColumnID>;
using ColumnIdToIndex = google::dense_hash_map<ColumnID, size_t>;
constexpr ColumnID EmptyColumnID = InvalidColumnID - 1;
constexpr ColumnID DeleteColumnID = EmptyColumnID - 1;

Field GenCustomField(const TiDB::ColumnInfo &);

std::optional<Field> GenFieldByColumnInfo(const TiDB::ColumnInfo & column)
{
    if (column.hasNoDefaultValueFlag())
    {
        if (column.hasNotNullFlag())
            return {};
        else
            return Field();
    }
    else
        return column.defaultValueToField();
}

struct ValueDecodeHelper
{
    const TiDB::TableInfo & table_info;
    const ColumnIdToIndex & schema_all_column_ids;

    bool forceDecodeTiKVValue(DecodedRow & decoded_row, DecodedRow & unknown)
    {
        bool schema_match = decoded_row.size() == schema_all_column_ids.size();
        bool has_dropped_column = false;

        for (auto it = decoded_row.cbegin(); schema_match && it != decoded_row.cend(); ++it)
        {
            if (!schema_all_column_ids.count(it->col_id))
                schema_match = false;
        }

        if (schema_match)
            return has_dropped_column;

        DecodedRow tmp_row;
        {
            tmp_row.reserve(schema_all_column_ids.size());
            for (auto && item : decoded_row)
            {
                if (schema_all_column_ids.count(item.col_id))
                    tmp_row.emplace_back(std::move(item));
                else
                    unknown.emplace_back(std::move(item));
            }
            decoded_row.clear();
        }

        {
            // must be sorted.
            ::std::sort(tmp_row.begin(), tmp_row.end());
            ::std::sort(unknown.begin(), unknown.end());
        }

        DecodedRowElement tmp_ele(InvalidColumnID, {});

        for (const auto & column_index : schema_all_column_ids)
        {
            const auto & column = table_info.columns[column_index.second];
            {
                tmp_ele.col_id = column.id;
                if (auto it = tmp_ele.findByColumnID(tmp_row); it != tmp_row.end())
                {
                    decoded_row.emplace_back(std::move(*it));
                    continue;
                }
            }
            auto field = GenFieldByColumnInfo(column);
            if (!field)
                has_dropped_column = true;
            else
                decoded_row.emplace_back(column.id, std::move(*field));
        }

        ::std::sort(decoded_row.begin(), decoded_row.end());
        return has_dropped_column;
    }
};

static inline void forceDecodeTiKVValue(const TiKVValue & value, ValueDecodeHelper & helper)
{
    auto & decoded_row_info = value.extraInfo();
    if (decoded_row_info.load())
        return;

    DecodedRow decoded_row, unknown;
    // TODO: support fast codec of tidb
    {
        size_t cursor = 0;

        const auto & raw_value = value.getStr();
        while (cursor < raw_value.size())
        {
            Field f = DecodeDatum(cursor, raw_value);
            if (f.isNull())
                break;
            ColumnID col_id = f.get<ColumnID>();
            decoded_row.emplace_back(col_id, DecodeDatum(cursor, raw_value));
        }

        if (cursor != raw_value.size())
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": cursor is not end", ErrorCodes::LOGICAL_ERROR);

        {
            // must be sorted.
            ::std::sort(decoded_row.begin(), decoded_row.end());
        }

        auto has_dropped_column = helper.forceDecodeTiKVValue(decoded_row, unknown);
        DecodedRowBySchema * decoded_row_ptr = new DecodedRowBySchema(
            helper.table_info.schema_version, has_dropped_column, std::move(decoded_row), std::move(unknown), true);
        decoded_row_info.atomicUpdate(decoded_row_ptr);
    }
}

void tryPreDecodeTiKVValue(std::optional<ExtraCFDataQueue> && values, StorageMergeTree & storage)
{
    if (!values)
        return;

    auto table_lock = storage.lockStructure(false, __PRETTY_FUNCTION__);

    const auto & table_info = storage.getTableInfo();
    ColumnIdToIndex schema_all_column_ids;
    ColumnIds decoded_col_ids_set;

    {
        schema_all_column_ids.set_empty_key(EmptyColumnID);
        schema_all_column_ids.set_deleted_key(DeleteColumnID);
        decoded_col_ids_set.set_empty_key(EmptyColumnID);
        for (size_t i = 0; i < table_info.columns.size(); ++i)
        {
            auto & column = table_info.columns[i];
            if (table_info.pk_is_handle && column.hasPriKeyFlag())
                continue;
            schema_all_column_ids.insert({column.id, i});
        }
    }

    ValueDecodeHelper helper{table_info, schema_all_column_ids};
    for (const auto & value : *values)
        forceDecodeTiKVValue(*value, helper);
}

} // namespace DB
