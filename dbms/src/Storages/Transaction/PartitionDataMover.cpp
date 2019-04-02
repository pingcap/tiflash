#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/executeQuery.h>

#include <Storages/StorageMergeTree.h>
#include <Storages/MutableSupport.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/PartitionDataMover.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace PartitionDataMover
{
/*
BlockInputStreamPtr createBlockInputStreamFromRange(const Context& context, StorageMergeTree* storage,
                                                    const HandleID begin, const HandleID excluded_end)
{
    SortDescription pk_columns = storage->getData().getPrimarySortDescription();
    // TODO: make sure PKs are all uint64
    if (pk_columns.size() != 1)
        throw Exception("PartitionDataMover: primary key should be one column", ErrorCodes::LOGICAL_ERROR);
    std::string pk_name = pk_columns[0].column_name;
    Block sample = storage->getSampleBlock();
    ColumnWithTypeAndName & pk_info = sample.safeGetByPosition(sample.getPositionByName(pk_name));
    // TODO: dont use string name
    if (pk_info.type->getName() != "Int64")
        throw Exception("PartitionDataMover: primary key should be Int64", ErrorCodes::LOGICAL_ERROR);

    std::stringstream ss;
    ss << "SELRAW NOKVSTORE * FROM " << storage->getDatabaseName() << "." << storage->getTableName() << " WHERE (" <<
        begin << " <= " << pk_name << ") AND (" <<
        pk_name << " < " << excluded_end << ")";
    std::string query = ss.str();

    LOG_DEBUG(&Logger::get("PartitionDataMover"), "createBlockInputStreamFromRange sql: " << query);

    Context query_context = context;
    query_context.setSessionContext(query_context);
    return executeQuery(query, query_context, true, QueryProcessingStage::Complete).in;
}

std::string columnNames(const Block & block, bool show_type = true)
{
    std::stringstream ss;
    ss << "[";
    for (const auto & col: block.getNamesAndTypesList())
    {
        ss << col.name;
        if (show_type)
            ss << "(" << col.type->getName() << ")";
        ss << ", ";
    }
    std::string str = ss.str();
    if (!block.getNames().empty())
        str.resize(str.size() - 2);
    return str + "]";
}

void markDeleteAllInBlock(Block & block)
{
    if (!block.has(MutableSupport::delmark_column_name))
    {
        throw Exception("PartitionDataMover: block without delmark_column_name. Columns: " + columnNames(block),
            ErrorCodes::LOGICAL_ERROR);
    }

    size_t delmark_col_pos = block.getPositionByName(MutableSupport::delmark_column_name);
    ColumnWithTypeAndName & delmark_col_info = block.safeGetByPosition(delmark_col_pos);
    auto delmark_type = delmark_col_info.type;
    auto delmark_new_col = delmark_col_info.column->cloneResized(delmark_col_info.column->size());
    ColumnUInt8 * delmark_col = typeid_cast<ColumnUInt8 *>(delmark_new_col.get());
    ColumnUInt8::Container & delmark_data = delmark_col->getData();
    for (size_t i = 0; i < delmark_data.size(); ++i)
        delmark_data[i] = MutableSupport::DelMark::genDelMark(false, true, delmark_data[i]);
    block.erase(delmark_col_pos);
    block.insert(delmark_col_pos, ColumnWithTypeAndName{std::move(delmark_new_col),
        delmark_type, MutableSupport::delmark_column_name});
}

void increaseVersionInBlock(Block & block, size_t increasement = 1)
{
    if (!block.has(MutableSupport::version_column_name))
    {
        throw Exception("PartitionDataMover: block without version_column_name. Columns: " + columnNames(block),
            ErrorCodes::LOGICAL_ERROR);
    }

    size_t version_col_pos = block.getPositionByName(MutableSupport::version_column_name);
    ColumnWithTypeAndName & version_col_info = block.safeGetByPosition(version_col_pos);
    auto version_type = version_col_info.type;
    auto version_new_col = version_col_info.column->cloneResized(version_col_info.column->size());
    ColumnUInt64 * version_col = typeid_cast<ColumnUInt64 *>(version_new_col.get());
    ColumnUInt64::Container & version_data = version_col->getData();
    const auto & version_old_col = *(version_col_info.column.get());
    // TODO: must be a faster way to avoid Field converting
    for (size_t i = 0; i < version_data.size(); ++i)
        version_data[i] = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), version_old_col[i]) + increasement;
    block.erase(version_col_pos);
    block.insert(version_col_pos, ColumnWithTypeAndName{std::move(version_new_col),
        version_type, MutableSupport::version_column_name});
}
*/
} // namespace PartitionDataMover
/*
void deleteRange(const Context& context, StorageMergeTree* storage,
                 const HandleID begin, const HandleID excluded_end)
{
    auto table_lock = storage->lockStructure(true, __PRETTY_FUNCTION__);

    BlockInputStreamPtr input = PartitionDataMover::createBlockInputStreamFromRange(
        context, storage, begin, excluded_end);
    TxnMergeTreeBlockOutputStream output(*storage);

    output.writePrefix();

    while (true)
    {
        Block block = input->read();
        if (!block)
            break;
        PartitionDataMover::markDeleteAllInBlock(block);
        PartitionDataMover::increaseVersionInBlock(block);
        output.write(block);
    }
    output.writeSuffix();
}

// TODO: use `new_ver = old_ver+1` to delete data is not a good way, may conflict with data in the future
void moveRangeBetweenPartitions(const Context & context, StorageMergeTree * storage,
    UInt64 src_partition_id, UInt64 dest_partition_id, const Field & begin, const Field & excluded_end)
{
    if (src_partition_id == dest_partition_id)
    {
        LOG_DEBUG(&Logger::get("PartitionDataMover"), "Partition " << src_partition_id <<
            " equal " << dest_partition_id << ", skipped moving");
        return;
    }

    // TODO: we should lock all
    auto table_lock = storage->lockStructure(true, __PRETTY_FUNCTION__);

    BlockInputStreamPtr input = PartitionDataMover::createBlockInputStreamFromRangeInPartition(
        context, storage, src_partition_id, begin, excluded_end);
    TxnMergeTreeBlockOutputStream del_output(*storage, src_partition_id);
    TxnMergeTreeBlockOutputStream dest_output(*storage, dest_partition_id);

    del_output.writePrefix();
    dest_output.writePrefix();

    size_t count = 0;
    while (true)
    {
        Block block = input->read();
        if (!block || block.rows() == 0)
            break;
        count += block.rows();
        PartitionDataMover::increaseVersionInBlock(block);
        dest_output.write(block);
        PartitionDataMover::markDeleteAllInBlock(block);
        del_output.write(block);
    }
    del_output.writeSuffix();
    dest_output.writeSuffix();

    LOG_DEBUG(&Logger::get("PartitionDataMover"),
        "Moved " << count << " raw rows from partition " << src_partition_id << " to " <<
        dest_partition_id << ", range: [" << applyVisitor(FieldVisitorToString(), begin) <<
        ", " << applyVisitor(FieldVisitorToString(), excluded_end) << ")");
}
*/

}
