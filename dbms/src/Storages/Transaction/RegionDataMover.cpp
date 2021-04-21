#include <Interpreters/executeQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TABLE_IS_DROPPED;
}

static const std::string RegionDataMoverName = "RegionDataMover";

template <typename HandleType>
BlockInputStreamPtr createBlockInputStreamFromRange(
    Context & context, const StorageMergeTree & storage, const HandleRange<HandleType> & handle_range, const std::string & pk_name)
{
    std::stringstream ss;
    ss << "SELRAW NOKVSTORE `" << MutableSupport::version_column_name << "`, `" << MutableSupport::delmark_column_name << "`, `" << pk_name
       << "` FROM `" << storage.getDatabaseName() << "`.`" << storage.getTableName() << "` WHERE (" << handle_range.first.handle_id
       << " <= `" << pk_name << "`) AND (`" << pk_name << "`";

    if (handle_range.second.type == TiKVHandle::HandleIDType::NORMAL)
        ss << " < " << handle_range.second.handle_id << ")";
    else
        ss << " <= " << std::numeric_limits<HandleType>::max() << ")";

    std::string query = ss.str();

    LOG_DEBUG(&Logger::get(RegionDataMoverName), __FUNCTION__ << ": sql " << query);

    return executeQuery(query, context, true, QueryProcessingStage::Complete).in;
}

template <typename HandleType>
void getHandleMapByRange(
    Context & context, StorageMergeTree & storage, const HandleRange<HandleType> & handle_range, HandleMap & output_data)
{
    SortDescription pk_columns = storage.getData().getPrimarySortDescription();
    if (pk_columns.size() != 1)
        throw Exception(std::string(__PRETTY_FUNCTION__) + ": primary key should be one column", ErrorCodes::LOGICAL_ERROR);

    std::string pk_name = pk_columns[0].column_name;
    auto start_time = Clock::now();

    BlockInputStreamPtr input = createBlockInputStreamFromRange(context, storage, handle_range, pk_name);

    size_t delmark_col_pos, version_col_pos, pk_col_pos;
    {
        Block sample = input->getHeader();
        delmark_col_pos = sample.getPositionByName(MutableSupport::delmark_column_name);
        version_col_pos = sample.getPositionByName(MutableSupport::version_column_name);
        pk_col_pos = sample.getPositionByName(pk_name);
    }

    size_t tol_rows = 0;

    while (true)
    {
        Block block = input->read();
        if (!block)
            break;
        size_t rows = block.rows();
        tol_rows += rows;

        const UInt8 * delmark_col = static_cast<const ColumnUInt8 *>(block.getByPosition(delmark_col_pos).column.get())->getData().data();
        const UInt64 * version_col = static_cast<const ColumnUInt64 *>(block.getByPosition(version_col_pos).column.get())->getData().data();
        const auto handle_col = block.getByPosition(pk_col_pos).column;
        for (size_t idx = 0; idx < rows; ++idx)
        {
            UInt8 delmark = delmark_col[idx];
            UInt64 version = version_col[idx];
            HandleID handle = handle_col->getInt(idx);
            const HandleMap::mapped_type cur_ele = {version, delmark};
            auto [it, ok] = output_data.emplace(handle, cur_ele);
            if (!ok)
            {
                auto & ele = it->second;
                ele = std::max(ele, cur_ele);
            }
        }
    }

    auto end_time = Clock::now();
    LOG_DEBUG(&Logger::get(RegionDataMoverName),
        __FUNCTION__ << ": execute sql and handle data, cost "
                     << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << " ms, read " << tol_rows
                     << " rows");
}

template void getHandleMapByRange<Int64>(Context &, StorageMergeTree &, const HandleRange<Int64> &, HandleMap &);
template void getHandleMapByRange<UInt64>(Context &, StorageMergeTree &, const HandleRange<UInt64> &, HandleMap &);

void tryOptimizeStorageFinal(Context & context, TableID table_id)
{
    auto log = &Logger::get(RegionDataMoverName);
    auto & tmt = context.getTMTContext();
    auto storage = tmt.getStorages().get(table_id);
    if (!storage)
    {
        LOG_INFO(log, __FUNCTION__ << ": storage " << table_id << " is none");
        return;
    }

    // Only for TMT
    ManageableStoragePtr managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
    if (!managed_storage || managed_storage->engineType() != TiDB::StorageEngine::TMT)
        return;

    try
    {
        auto table_lock = managed_storage->lockStructureForShare(RWLock::NO_QUERY);
    }
    catch (DB::Exception & e)
    {
        // We can ignore if storage is dropped.
        if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
            return;
        else
            throw;
    }

    std::stringstream ss;
    ss << "OPTIMIZE TABLE `" << storage->getDatabaseName() << "`.`" << storage->getTableName() << "` PARTITION ID '0' FINAL";

    std::string query = ss.str();

    LOG_WARNING(log, __FUNCTION__ << ": execute sql " << query);

    executeQuery(query, context, true, QueryProcessingStage::Complete);
}

} // namespace DB
