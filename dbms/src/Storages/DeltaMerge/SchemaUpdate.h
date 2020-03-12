#pragma once

#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>


namespace DB
{
namespace DM
{

void setColumnDefineDefaultValue(const AlterCommand & command, ColumnDefine & define);
void applyAlter(ColumnDefines &               table_columns,
                const AlterCommand &          command,
                const OptionTableInfoConstRef table_info,
                ColumnID &                    max_column_id_used);

} // namespace DM
} // namespace DB