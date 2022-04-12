#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class MockExecutorTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeDAGContext();
        DAGRequestBuilder::executor_index = 0;
    }

public:
    MockExecutorTest()
        : context(TiFlashTestEnv::getContext())
    {}

    virtual void initializeDAGContext()
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        context.setDAGContext(dag_context_ptr.get());
    }

    DAGContext & getDAGContext()
    {
        assert(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

protected:
    Context context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

} // namespace tests
} // namespace DB