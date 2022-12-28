#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>

#include <unordered_map>

namespace DB::DM
{

dtpb::TiFlashSchema toRemote(const Block & schema)
{
    dtpb::TiFlashSchema remote_schema;
    for (const auto & col : schema)
    {
        auto * remote_col = remote_schema.add_columns();
        remote_col->set_column_id(col.column_id);
        remote_col->set_type_full_name(col.type->getName());
        remote_col->set_column_name(col.name);
    }
    return remote_schema;
}

std::tuple<BlockPtr, std::unordered_map<ColId, size_t>>
fromRemote(const dtpb::TiFlashSchema & remote_schema)
{
    ColumnsWithTypeAndName cols;
    cols.reserve(remote_schema.columns_size());
    std::unordered_map<ColId, size_t> colid_to_offset;

    auto & factory = DataTypeFactory::instance();
    for (int index = 0; index < remote_schema.columns_size(); ++index)
    {
        const auto & remote_col = remote_schema.columns(index);
        auto data_type = factory.getOrSet(remote_col.type_full_name());
        cols.emplace_back(
            nullptr,
            data_type,
            remote_col.column_name(),
            remote_col.column_id());
        colid_to_offset[remote_col.column_id()] = static_cast<size_t>(index);
    }
    BlockPtr block = std::make_shared<Block>(std::move(cols));
    return {std::move(block), std::move(colid_to_offset)};
}

} // namespace DB::DM
