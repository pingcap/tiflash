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

#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class DAGWarnings : public DB::tests::FunctionTest
{
public:
    void initializeDAGContext() override
    {
        dag_context_ptr = std::make_unique<DAGContext>(5);
        context->setDAGContext(dag_context_ptr.get());
    }
};

/// only WarningsTruncated needs to be tested explicitly
TEST_F(DAGWarnings, WarningsTruncated)
try
{
    const std::string func_name = "formatWithLocale";
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"0.0000",
             "-0.0120",
             "0.0120",
             "12,332.1235",
             "12,332.1235",
             "12,332.1235",
             "12,332.1235",
             "12,332.1235",
             {},
             {},
             {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>(
                {0,
                 -.012,
                 .012,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 12332.123456,
                 {},
                 {}}),
            createColumn<Nullable<Int64>>({4, 4, 4, 4, 4, 4, 4, 4, {}, 4, {}}),
            createColumn<Nullable<String>>(
                {"en_US", "en_US", "en_US", "en_US", "en_us", "xxx", "xx1", {}, "xx2", "xx3", "xx4"})));

    auto gen_warning_str = [](const std::string & value) -> std::string {
        return fmt::format("Unknown locale: \'{}\'", value);
    };
    std::vector<std::string> expected_warnings{
        gen_warning_str("xxx"),
        gen_warning_str("xx1"),
        gen_warning_str("NULL"),
        gen_warning_str("xx2"),
        gen_warning_str("xx3")};
    std::vector<tipb::Error> actual_warnings;
    getDAGContext().consumeWarnings(actual_warnings);
    ASSERT_TRUE(getDAGContext().getWarningCount() == 6);
    ASSERT_TRUE(expected_warnings.size() == actual_warnings.size());
    for (size_t i = 0; i < expected_warnings.size(); ++i)
    {
        auto actual_warning = actual_warnings[i];
        ASSERT_TRUE(actual_warning.has_msg() && actual_warning.msg() == expected_warnings[i]);
    }
}
CATCH

} // namespace DB::tests
