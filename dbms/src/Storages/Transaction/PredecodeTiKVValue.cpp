#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/PredecodeTiKVValue.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RowCodec.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

template <bool skip>
void Region::tryPreDecodeTiKVValue(TMTContext & tmt)
{
    auto table_id = getMappedTableID();
    auto storage = tmt.getStorages().get(table_id);
    if (!storage)
        return;

    std::optional<CFDataPreDecodeQueue> default_val, write_val;
    {
        std::lock_guard<std::mutex> predecode_lock(predecode_mutex);
        default_val = data.defaultCF().getCFDataPreDecode().popAll();
        write_val = data.writeCF().getCFDataPreDecode().popAll();
    }

    /// just clean up pre-decode queue
    if (skip)
        return;

    DB::tryPreDecodeTiKVValue(std::move(default_val), storage);
    DB::tryPreDecodeTiKVValue(std::move(write_val), storage);
}

template void Region::tryPreDecodeTiKVValue<true>(TMTContext &);
template void Region::tryPreDecodeTiKVValue<false>(TMTContext &);

void RowPreDecoder::preDecodeRow(const TiKVValue & value)
{
    auto & decoded_row = value.getDecodedRow();
    if (decoded_row.load())
        return;

    auto * decoded_row_ptr = decodeRow(value.getStr(), table_info, column_lut);

    decoded_row.atomicUpdate(decoded_row_ptr);
}

void tryPreDecodeTiKVValue(std::optional<CFDataPreDecodeQueue> && values, const ManageableStoragePtr & storage)
{
    if (!values)
        return;

    auto table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);

    const auto & table_info = storage->getTableInfo();
    ColumnIdToIndex column_lut;

    {
        column_lut.set_empty_key(EmptyColumnID);
        column_lut.set_deleted_key(DeleteColumnID);
        for (size_t i = 0; i < table_info.columns.size(); ++i)
        {
            auto & column = table_info.columns[i];
            if (table_info.pk_is_handle && column.hasPriKeyFlag())
                continue;
            column_lut.insert({column.id, i});
        }
    }

    RowPreDecoder preDecoder{table_info, column_lut};
    for (const auto & value : *values)
        preDecoder.preDecodeRow(*value);
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
