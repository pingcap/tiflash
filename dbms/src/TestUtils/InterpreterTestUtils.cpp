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

#include <Common/FmtUtils.h>
#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/executorSerializer.h>

namespace DB::tests
{
DAGContext & MockExecutorTest::getDAGContext()
{
    assert(dag_context_ptr != nullptr);
    return *dag_context_ptr;
}

void MockExecutorTest::initializeContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context = MockDAGRequestContext(TiFlashTestEnv::getContext());
}

void MockExecutorTest::SetUpTestCase()
{
    try
    {
        DB::registerFunctions();
        DB::registerAggregateFunctions();
    }
    catch (DB::Exception &)
    {
        // Maybe another test has already registered, ignore exception here.
    }
}

void MockExecutorTest::dagRequestEqual(String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    FmtBuffer buf;
    auto serializer = ExecutorSerializer(context.context, buf);
    ASSERT_EQ(Poco::trimInPlace(expected_string), Poco::trim(serializer.serialize(actual.get())));
}
} // namespace DB::tests
