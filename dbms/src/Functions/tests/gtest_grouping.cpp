// Copyright 2023 PingCAP, Ltd.
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

#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace tests
{

struct MetaData
{
    UInt32 version;
    UInt64 grouping_id;
    std::set<UInt64> grouping_ids;
};



class TestGrouping : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName executeGroupingFunction(
        const ColumnNumbers & argument_column_numbers,
        const ColumnsWithTypeAndName & columns,
        const MetaData & meta_data)
    {
        auto & factory = FunctionFactory::instance();

        ColumnsWithTypeAndName arguments;
        for (const auto argument_column_number : argument_column_numbers)
            arguments.push_back(columns.at(argument_column_number));

        auto builder = factory.tryGet(grouping_func_name, *context);
        if (!builder)
            throw TiFlashTestException(fmt::format("Function {} not found!", grouping_func_name));

        Block block(columns);
        auto func = builder->build(arguments, nullptr);

        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, argument_column_numbers, columns.size());

        return block.getByPosition(columns.size());
    }

private:
    const String grouping_func_name = "grouping";
};

TEST_F(TestGrouping, TestVersion1)
try
{

}
CATCH

TEST_F(TestGrouping, TestVersion2)
try
{

}
CATCH

TEST_F(TestGrouping, TestVersion3)
try
{

}
CATCH

} // namespace tests
} // namespace DB
