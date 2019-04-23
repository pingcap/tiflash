#pragma once

#include <Core/Names.h>
#include <Storages/Transaction/RegionDataRead.h>

namespace TiDB
{
struct TableInfo;
};

namespace DB
{

struct ColumnsDescription;
class Block;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

Block RegionBlockRead(const TiDB::TableInfo & table_info, const ColumnsDescription & columns, const Names & ordered_columns_,
    RegionDataReadInfoList & data_list);

} // namespace DB
