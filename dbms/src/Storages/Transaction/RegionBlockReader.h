#pragma once

#include <Common/typeid_cast.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using ScannerPtr = std::unique_ptr<Region::CommittedScanRemover>;

Block RegionBlockRead(const TiDB::TableInfo & table_info,
    const ColumnsDescription & columns, const Names & ordered_columns_, ScannerPtr & curr_scanner, std::vector<TiKVKey> * keys= nullptr);

} // namespace DB
