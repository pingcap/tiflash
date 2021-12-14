#pragma once

#include <Databases/IDatabase.h>
#include <Debug/DBGInvoker.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Parsers/IAST.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Types.h>

#include "dbgTools.h"
namespace DB
{
class RegionMockTest
{
public:
    RegionMockTest(KVStorePtr kvstore_, RegionPtr region_);
    ~RegionMockTest();

private:
    TiFlashRaftProxyHelper mock_proxy_helper;
    KVStorePtr kvstore;
    RegionPtr region;
};

void GenMockSSTDataMVCC(const TiDB::TableInfo & table_info,
                        TableID table_id,
                        const String & store_key,
                        UInt64 start_handle,
                        UInt64 end_handle,
                        size_t version_num,
                        const std::unordered_set<ColumnFamilyType> & cfs = {ColumnFamilyType::Write, ColumnFamilyType::Default});
} // namespace DB
