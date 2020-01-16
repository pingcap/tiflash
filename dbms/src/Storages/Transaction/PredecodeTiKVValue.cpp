#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/PredecodeTiKVValue.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/IManageableStorage.h>

namespace DB
{

void Region::tryPreDecodeTiKVValue(Context & context)
{
    auto table_id = getMappedTableID();
    auto & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().get(table_id);
    if (!storage)
        return;

    std::optional<CFDataPreDecodeQueue> default_val, write_val;
    {
        std::lock_guard<std::mutex> predecode_lock(predecode_mutex);
        default_val = data.defaultCF().getCFDataPreDecode().popAll();
        write_val = data.writeCF().getCFDataPreDecode().popAll();
    }

    DB::tryPreDecodeTiKVValue(std::move(default_val), storage);
    DB::tryPreDecodeTiKVValue(std::move(write_val), storage);
}

bool ValueDecodeHelper::forceDecodeTiKVValue(DecodedFields & decoded_fields, DecodedFields & unknown)
{
    bool schema_match = decoded_fields.size() == schema_all_column_ids.size();
    bool has_missing_columns = false;

    for (auto it = decoded_fields.cbegin(); schema_match && it != decoded_fields.cend(); ++it)
    {
        if (!schema_all_column_ids.count(it->col_id))
            schema_match = false;
    }

    if (schema_match)
        return has_missing_columns;

    {
        DecodedFields tmp_row;
        tmp_row.reserve(decoded_fields.size());
        for (auto && item : decoded_fields)
        {
            if (schema_all_column_ids.count(item.col_id))
                tmp_row.emplace_back(std::move(item));
            else
                unknown.emplace_back(std::move(item));
        }

        // must be sorted, for binary search.
        ::std::sort(tmp_row.begin(), tmp_row.end());
        ::std::sort(unknown.begin(), unknown.end());
        tmp_row.swap(decoded_fields);
    }

    for (const auto & column_index : schema_all_column_ids)
    {
        const auto & column = table_info.columns[column_index.second];
        if (auto it = findByColumnID(column.id, decoded_fields); it != decoded_fields.end())
            continue;

        if (column.hasNoDefaultValueFlag() && column.hasNotNullFlag())
        {
            has_missing_columns = true;
            break;
        }
    }

    return has_missing_columns;
}

void ValueDecodeHelper::forceDecodeTiKVValue(const TiKVValue & value)
{
    auto & decoded_fields_info = value.getDecodedRow();
    if (decoded_fields_info.load())
        return;

    DecodedFields decoded_fields, unknown;
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
            decoded_fields.emplace_back(col_id, DecodeDatum(cursor, raw_value));
        }

        if (cursor != raw_value.size())
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": cursor is not end", ErrorCodes::LOGICAL_ERROR);

        {
            // must be sorted, for binary search.
            ::std::sort(decoded_fields.begin(), decoded_fields.end());
        }

        auto has_missing_columns = forceDecodeTiKVValue(decoded_fields, unknown);
        DecodedRow * decoded_fields_ptr = new DecodedRow(has_missing_columns, std::move(unknown), true, std::move(decoded_fields));
        decoded_fields_info.atomicUpdate(decoded_fields_ptr);
    }
}

void tryPreDecodeTiKVValue(std::optional<CFDataPreDecodeQueue> && values, const ManageableStoragePtr & storage)
{
    if (!values)
        return;

    auto table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);

    const auto & table_info = storage->getTableInfo();
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

DecodedFields::const_iterator findByColumnID(Int64 col_id, const DecodedFields & row)
{
    const static auto cmp = [](const DecodedField & e, const Int64 cid) -> bool { return e.col_id < cid; };
    auto it = std::lower_bound(row.cbegin(), row.cend(), col_id, cmp);
    if (it != row.cend() && it->col_id == col_id)
        return it;
    return row.cend();
}

} // namespace DB
