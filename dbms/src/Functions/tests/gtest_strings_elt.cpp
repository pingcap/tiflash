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

#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestFunctionElt : public DB::tests::FunctionTest
{
};

#define ASSERT_ELT(result, t1, t2, ...) \
    ASSERT_COLUMN_EQ(result, executeFunction("elt", t1, t2, __VA_ARGS__))

TEST_F(TestFunctionElt, BoundaryIdx)
try
{
    ASSERT_ELT(createColumn<Nullable<String>>({{}, {}, {}, {}, {}, {}}),
        createColumn<Nullable<Int64>>({{}, -1, -2, -100, 3, 100}),
        createColumn<Nullable<String>>({"abc", "123", "", {}, "", ""}),
        createColumn<Nullable<String>>({"def", "123", {}, "", "", ""}));
}
CATCH

TEST_F(TestFunctionElt, AllTypeIdx)
try
{

}
CATCH

TEST_F(TestFunctionElt, Simple)
try
{
    // const idx


    // vector idx


}
CATCH

TEST_F(TestFunctionElt, Random)
try
{

}
CATCH

} // namespace tests
} // namespace DB
