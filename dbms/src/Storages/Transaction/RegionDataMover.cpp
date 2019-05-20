#include <Interpreters/executeQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/RegionDataMover.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <typename HandleType>
BlockInputStreamPtr createBlockInputStreamFromRange(
    Context & context, const StorageMergeTree & storage, const HandleRange<HandleType> & handle_range, const std::string & pk_name)
{
    std::stringstream ss;
    ss << "SELRAW NOKVSTORE " << MutableSupport::version_column_name << ", " << MutableSupport::delmark_column_name << ", " << pk_name
       << " FROM " << storage.getDatabaseName() << "." << storage.getTableName() << " WHERE (" << handle_range.first.handle_id
       << " <= " << pk_name << ") AND (" << pk_name;

    if (handle_range.second.type == TiKVHandle::HandleIDType::NORMAL)
        ss << " < " << handle_range.second.handle_id << ")";
    else
        ss << " <= " << std::numeric_limits<HandleType>::max() << ")";

    std::string query = ss.str();

    LOG_DEBUG(&Logger::get("RegionDataMover"), "createBlockInputStreamFromRange sql: " << query);

    return executeQuery(query, context, true, QueryProcessingStage::Complete).in;
}

template <typename HandleType>
HandleMap getHandleMapByRange(Context & context, StorageMergeTree & storage, const HandleRange<HandleType> & handle_range)
{
    SortDescription pk_columns = storage.getData().getPrimarySortDescription();
    if (pk_columns.size() != 1)
        throw Exception("RegionDataMover: primary key should be one column", ErrorCodes::LOGICAL_ERROR);

    std::string pk_name = pk_columns[0].column_name;

    BlockInputStreamPtr input = createBlockInputStreamFromRange(context, storage, handle_range, pk_name);

    size_t delmark_col_pos, version_col_pos, pk_col_pos;
    {
        Block sample = input->getHeader();
        delmark_col_pos = sample.getPositionByName(MutableSupport::delmark_column_name);
        version_col_pos = sample.getPositionByName(MutableSupport::version_column_name);
        pk_col_pos = sample.getPositionByName(pk_name);
    }

    HandleMap output_data;

    while (true)
    {
        Block block = input->read();
        if (!block)
            break;
        size_t rows = block.rows();

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

    return output_data;
}

template HandleMap getHandleMapByRange<Int64>(Context & context, StorageMergeTree & storage, const HandleRange<Int64> & handle_range);
template HandleMap getHandleMapByRange<UInt64>(Context & context, StorageMergeTree & storage, const HandleRange<UInt64> & handle_range);

} // namespace DB
