#include <Debug/dbgFuncMockRaftSnapshot.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/File.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/TMTContext.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
static void generateSSTToDTFiles(Context & context, const TiDB::TableInfo & table_info, RegionID region_id, RegionID start_handle, RegionID end_handle, std::shared_ptr<StorageDeltaMerge> storage, size_t version_num)
{
    auto store = storage->getStore();

    const auto region_name = "__snap_snap_" + std::to_string(region_id);
    GenMockSSTDataMVCC(table_info, table_info.id, region_name, start_handle, end_handle, version_num, {ColumnFamilyType::Write, ColumnFamilyType::Default, ColumnFamilyType::Lock});

    auto & tmt = context.getTMTContext();
    auto & kv_store = tmt.getKVStore();

    RegionPtr new_region = RegionBench::createRegion(table_info.id, region_id, start_handle, end_handle + 10000, 8);
    // Register some mock SST reading methods so that we can decode data in `MockSSTReader::MockSSTData`
    RegionMockTest mock_test(kv_store, new_region);

    std::vector<SSTView> sst_views;
    {
        sst_views.push_back(SSTView{
            ColumnFamilyType::Write,
            BaseBuffView{region_name.data(), region_name.length()},
        });
        sst_views.push_back(SSTView{
            ColumnFamilyType::Default,
            BaseBuffView{region_name.data(), region_name.length()},
        });
        sst_views.push_back(SSTView{
            ColumnFamilyType::Lock,
            BaseBuffView{region_name.data(), region_name.length()},
        });
    }

    PageIds ids = kv_store->preHandleSnapshotToFiles(new_region, SSTViewVec{sst_views.data(), sst_views.size()}, 0, 0, tmt);
    EXPECT_GT(ids.size(), 0);

    auto scanner = new_region->createCommittedScanner();
    RegionLockReadQuery lock_query{200, nullptr};
    DecodedLockCFValuePtr lock_value = scanner.getLockInfo(lock_query);
    EXPECT_EQ(lock_value->lock_version, 100);

    storage->ingestFiles({DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())}, ids, true, context.getSettingsRef());
}

static void preHandleSSTFiles(Context & context, RegionID region_id, RegionID start_handle, RegionID end_handle, size_t version_num)
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"col_1","O":"col_1"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}},
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}}
        ],
        "pk_is_handle":true,"index_info":[],"is_common_handle":false,
        "name":{"L":"test","O":"test"},"partition":null,
        "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
    })json";
    TiDB::TableInfo table_info(table_info_json);
    table_info.columns[0].setPriKeyFlag();

    std::shared_ptr<StorageDeltaMerge> storage;
    DataTypes data_types;
    Names column_names;
    // create table
    {
        NamesAndTypesList names_and_types_list{
            {"col_1", std::make_shared<DataTypeInt64>()},
            {"col_2", std::make_shared<DataTypeInt64>()},
        };
        for (const auto & name_type : names_and_types_list)
        {
            data_types.push_back(name_type.type);
            column_names.push_back(name_type.name);
        }

        const String path_name = DB::tests::TiFlashTestEnv::getTemporaryPath("SSTFiles2DTFiles");
        Poco::File path(path_name);
        if (path.exists())
            path.remove(true);

        // primary_expr_ast
        ASTPtr astptr(new ASTIdentifier(table_info.name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col_1"));

        storage = StorageDeltaMerge::create("TiFlash",
                                            /* db_name= */ "default",
                                            table_info.name,
                                            table_info,
                                            ColumnsDescription{names_and_types_list},
                                            astptr,
                                            0,
                                            context);
        storage->startup();
    }

    generateSSTToDTFiles(context, table_info, region_id, start_handle, end_handle, storage, version_num);

    QueryProcessingStage::Enum stage;
    SelectQueryInfo query_info;
    query_info.query = std::make_shared<ASTSelectQuery>();
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(context.getSettingsRef().resolve_locks, std::numeric_limits<UInt64>::max());
    BlockInputStreams ins = storage->read(column_names, query_info, context, stage, 8192, 1);
    ASSERT_EQ(ins.size(), 1);
    BlockInputStreamPtr in = ins[0];
    in->readPrefix();

    size_t num_rows_read = 0;
    while (Block block = in->read())
    {
        num_rows_read += block.rows();
        for (auto & iter : block)
        {
            auto c = iter.column;
            for (size_t i = 0; i < c->size(); i++)
            {
                if (iter.name == "col_2")
                {
                    EXPECT_EQ(c->getInt(i), -((i + 1) * 100 + version_num));
                }
            }
        }
    }
    in->readSuffix();
    EXPECT_EQ(num_rows_read, end_handle - start_handle);

    storage->drop();
    storage->removeFromTMTContext();
}

TEST(SSTFiles2DTFiles, testSSTToDTFiles)
{
    auto ctx = TiFlashTestEnv::getContext();
    preHandleSSTFiles(ctx, /*region_id*/ 4, /*start_handle*/ 1, /*end_handle*/ 10, /*version_num*/ 1);
    // test mvcc
    preHandleSSTFiles(ctx, /*region_id*/ 4, /*start_handle*/ 1, /*end_handle*/ 10, /*version_num*/ 10);
}

} // namespace tests
} // namespace DB