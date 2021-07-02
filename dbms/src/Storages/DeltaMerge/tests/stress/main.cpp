#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <Storages/DeltaMerge/tests/stress/DeltaMergeStoreProxy.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>

#include <memory>
#include <thread>
#include "SimpleLockManager.h"

#include <Common/FailPoint.h>

void usage(const char* program)
{
    fprintf(stderr, "Usage: %s <write_thread> <read_thread> <run_seconds> <gen_data_count>\n", program);
}

int main(int argc, [[maybe_unused]]char * argv[])
{
    if (argc < 4)
    {
        usage(argv[0]);
        exit(-1);
    }
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();
    fiu_init(0); // init failpoint
    
    Int32 write_thread_count = std::stoi(argv[1]);
    Int32 read_thread_count = std::stoi(argv[2]);
    [[maybe_unused]]Int32 run_seconds = std::stoi(argv[3]);
    UInt64 gen_data_count = std::stoul(argv[4]);

    DB::DM::tests::DeltaMergeStoreProxy store_proxy;

    try
    {
        if (gen_data_count > 0)
        {
            store_proxy.genDataMultiThread(gen_data_count, write_thread_count);
        }
        store_proxy.readDataMultiThread(read_thread_count);
    }
    catch (std::exception& e)
    {
        LOG_INFO(&Poco::Logger::get("DeltaMergeStoreProxy"), e.what());
    }
    catch (...)
    {
        LOG_INFO(&Poco::Logger::get("DeltaMergeStoreProxy"), "Unknow exception");   
    }
    DB::tests::TiFlashTestEnv::shutdown();
    return 0;
}
