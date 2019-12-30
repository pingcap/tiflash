#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/PredecodeTiKVValue.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

void Region::tryPreDecodeTiKVValue(Context & context)
{
    auto table_id = getMappedTableID();
    auto & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().get(table_id);
    if (!storage)
        return;

    std::optional<ExtraCFDataQueue> default_val, write_val;
    {
        std::lock_guard<std::mutex> predecode_lock(predecode_mutex);
        default_val = data.defaultCF().getExtra().popAll();
        write_val = data.writeCF().getExtra().popAll();
    }

    DB::tryPreDecodeTiKVValue(std::move(default_val), *storage);
    DB::tryPreDecodeTiKVValue(std::move(write_val), *storage);
}

bool ValueDecodeHelper::forceDecodeTiKVValue(DecodedRow & decoded_row, DecodedRow & unknown)
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

    {
        DecodedRow tmp_row;
        tmp_row.reserve(decoded_row.size());
        for (auto && item : decoded_row)
        {
            if (schema_all_column_ids.count(item.col_id))
                tmp_row.emplace_back(std::move(item));
            else
                unknown.emplace_back(std::move(item));
        }

        // must be sorted.
        ::std::sort(tmp_row.begin(), tmp_row.end());
        ::std::sort(unknown.begin(), unknown.end());
        tmp_row.swap(decoded_row);
    }

    for (const auto & column_index : schema_all_column_ids)
    {
        const auto & column = table_info.columns[column_index.second];
        if (auto it = findByColumnID(column.id, decoded_row); it != decoded_row.end())
            continue;

        if (column.hasNoDefaultValueFlag() && column.hasNotNullFlag())
        {
            has_dropped_column = true;
            break;
        }
    }

    return has_dropped_column;
}

void ValueDecodeHelper::forceDecodeTiKVValue(const TiKVValue & value)
{
    auto & decoded_row_info = value.extraInfo();
    if (decoded_row_info.load())
        return;

    DecodedRow decoded_row, unknown;
    // TODO: support fast codec of TiDB
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

        auto has_dropped_column = forceDecodeTiKVValue(decoded_row, unknown);
        DecodedRowBySchema * decoded_row_ptr
            = new DecodedRowBySchema(table_info.schema_version, has_dropped_column, std::move(decoded_row), std::move(unknown), true);
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
        helper.forceDecodeTiKVValue(*value);
}

DecodedRow::const_iterator findByColumnID(Int64 col_id, const DecodedRow & row)
{
    const DecodedRowElement * e = (DecodedRowElement *)((char *)(&col_id) - offsetof(DecodedRowElement, col_id));
    return e->findByColumnID(row);
}

} // namespace DB
