#include <Encryption/MockKeyManager.h>
#include <Server/RaftConfigParser.h>
#include <Storages/Transaction/TMTContext.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
std::unique_ptr<Context> TiFlashTestEnv::global_context = nullptr;

void TiFlashTestEnv::initializeGlobalContext()
{
    // set itself as global context
    global_context = std::make_unique<DB::Context>(DB::Context::createGlobal());
    global_context->setGlobalContext(*global_context);
    global_context->setApplicationType(DB::Context::ApplicationType::SERVER);

    global_context->initializeTiFlashMetrics();
    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(false);
    global_context->initializeFileProvider(key_manager, false);

    // Theses global variables should be initialized by the following order
    // 1. capacity
    // 2. path pool
    // 3. TMTContext

    Strings testdata_path = {getTemporaryPath()};
    global_context->initializePathCapacityMetric(0, testdata_path, {}, {}, {});

    auto paths = getPathPool(testdata_path);
    global_context->setPathPool(
        paths.first, paths.second, Strings{}, true, global_context->getPathCapacity(), global_context->getFileProvider());
    TiFlashRaftConfig raft_config;

    raft_config.ignore_databases = {"default", "system"};
    raft_config.engine = TiDB::StorageEngine::TMT;
    raft_config.disable_bg_flush = false;
    global_context->createTMTContext(raft_config, pingcap::ClusterConfig());

    global_context->setDeltaIndexManager(1024 * 1024 * 100 /*100MB*/);

    global_context->getTMTContext().restore();
}

Context TiFlashTestEnv::getContext(const DB::Settings & settings, Strings testdata_path)
{
    Context context = *global_context;
    context.setGlobalContext(*global_context);
    // Load `testdata_path` as path if it is set.
    const String root_path = testdata_path.empty() ? getTemporaryPath() : testdata_path[0];
    if (testdata_path.empty())
        testdata_path.push_back(root_path);
    context.setPath(root_path);
    auto paths = getPathPool(testdata_path);
    context.setPathPool(paths.first, paths.second, Strings{}, true, context.getPathCapacity(), context.getFileProvider());
    context.getSettingsRef() = settings;
    return context;
}

void TiFlashTestEnv::shutdown()
{
    global_context->getTMTContext().setStatusTerminated();
    global_context->shutdown();
    global_context.reset();
}

::testing::AssertionResult DataTypeCompare(const char * lhs_expr, const char * rhs_expr, const DataTypePtr & lhs, const DataTypePtr & rhs)
{
    if (lhs->equals(*rhs))
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs->getName(), rhs->getName(), false);
}

} // namespace DB::tests
