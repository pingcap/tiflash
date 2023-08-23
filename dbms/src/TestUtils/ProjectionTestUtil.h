// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

ExpressionActionsPtr buildChangeNullable(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    const ColumnNumbers & column_nullable_numbers);

std::pair<ExpressionActionsPtr, std::vector<String>> buildProjection(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    const ColumnNumbers & column_literal_numbers,
    const std::vector<Field> & val_fields,
    const ColumnNumbers & column_ref_numbers);

std::pair<ExpressionActionsPtr, std::vector<String>> buildLiteralProjection(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    const std::vector<Field> & val_fields);

class ProjectionTest : public ::testing::Test
{
protected:
    void SetUp() override { initializeDAGContext(); }

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
