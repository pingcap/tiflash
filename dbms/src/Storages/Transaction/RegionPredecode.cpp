#include <Interpreters/Context.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

extern void tryPreDecodeTiKVValue(std::optional<ExtraCFDataQueue> && values, StorageMergeTree & storage);

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

} // namespace DB
