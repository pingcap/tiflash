#include <Core/ColumnNumbers.h>
#include <Encryption/MockKeyManager.h>
#include <Functions/FunctionFactory.h>
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

ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnsWithTypeAndName & columns)
{
    const auto context = TiFlashTestEnv::getContext();
    auto & factory = FunctionFactory::instance();

    Block block(columns);
    ColumnNumbers cns;
    for (size_t i = 0; i < columns.size(); ++i)
        cns.push_back(i);

    auto bp = factory.tryGet(func_name, context);
    if (!bp)
        throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
    auto func = bp->build(columns);
    block.insert({nullptr, func->getReturnType(), "res"});
    func->execute(block, cns, columns.size());
    return block.getByPosition(columns.size());
}

void TiFlashTestBase::assertDataTypeEqual(const DataTypePtr & actual, const DataTypePtr & expect)
{
    ASSERT_EQ(actual->getName(), expect->getName());
}

void TiFlashTestBase::assertColumnEqual(
    const ColumnPtr & actual,
    const ColumnPtr & expect)
{
    ASSERT_EQ(actual->getName(), expect->getName());
    ASSERT_EQ(actual->isColumnNullable(), expect->isColumnNullable());
    ASSERT_EQ(actual->isColumnConst(), expect->isColumnConst());
    ASSERT_EQ(actual->size(), expect->size());

    for (size_t i = 0, size = actual->size(); i < size; ++i)
    {
        auto actual_field = (*actual)[i];
        auto expect_field = (*expect)[i];

        EXPECT_TRUE(actual_field == expect_field)
            << "Value " << i << " mismatch."
            << "\n  Actual: " << actual_field.toString()
            << "\nExpected: " << expect_field.toString();
    }
}

void TiFlashTestBase::assertColumnEqual(
    const ColumnWithTypeAndName & actual,
    const ColumnWithTypeAndName & expect)
{
    SCOPED_TRACE("assertColumnEqual");

    assertDataTypeEqual(actual.type, expect.type);
    assertColumnEqual(actual.column, expect.column);
}

} // namespace DB::tests
