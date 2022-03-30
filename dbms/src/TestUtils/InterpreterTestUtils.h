// Copyright 2022 PingCAP, Ltd.
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

#include <Functions/registerFunctions.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class InterpreterTest : public ::testing::Test
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
    InterpreterTest()
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
