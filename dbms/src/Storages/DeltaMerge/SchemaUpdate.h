#pragma once

#include <Storages/AlterCommands.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>


namespace DB
{
namespace DM
{

void setColumnDefineDefaultValue(const AlterCommand & command, ColumnDefine & define);
void setColumnDefineDefaultValue(const TiDB::TableInfo & table_info, ColumnDefine & define);
void applyAlter(ColumnDefines &               table_columns,
                const AlterCommand &          command,
                const OptionTableInfoConstRef table_info,
                ColumnID &                    max_column_id_used);

} // namespace DM
} // namespace DB