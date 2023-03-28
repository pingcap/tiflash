
#pragma once

#include <Core/ColumnNumbers.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/ExpressionActions.h>
#include <TestUtils/TiFlashTestBasic.h>


namespace DB
{
namespace tests
{

ColumnsWithTypeAndName executeLiteralProjection(
    Context & context,
    std::vector<String> literals,
    const ColumnsWithTypeAndName & columns);

std::pair<ExpressionActionsPtr, std::vector<String>> buildProjection(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    const ColumnNumbers & column_literal_numbers,
    std::vector<Field> val_fields,
    const ColumnNumbers & column_ref_numbers);

std::pair<ExpressionActionsPtr, std::vector<String>> buildLiteralProjection(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    std::vector<Field> val_fields);

class ProjectionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        initializeDAGContext();
    }

public:
    static void SetUpTestCase()
    {
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    }

    ProjectionTest();

    virtual void initializeDAGContext();

    ColumnsWithTypeAndName executeProjection(
        const std::vector<String> literals,
        const ColumnsWithTypeAndName & columns);


    DAGContext & getDAGContext()
    {
        RUNTIME_ASSERT(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

protected:
    ContextPtr context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};
} // namespace tests
} // namespace DB
