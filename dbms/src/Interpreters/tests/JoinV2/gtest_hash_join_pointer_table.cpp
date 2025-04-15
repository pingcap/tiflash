// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>
#include <Interpreters/JoinV2/HashJoinPointerTable.h>
#include <Interpreters/sortBlock.h>
#include <TestUtils/FunctionTestUtils.h>


namespace DB
{
namespace tests
{

class HashJoinPointerTableTest : public ::testing::Test
{
public:
    static void testInit(
        HashJoinKeyMethod method,
        size_t row_count,
        size_t hash_value_bytes,
        size_t pointer_table_size,
        size_t pointer_table_size_degree,
        size_t pointer_table_size_mask)
    {
        {
            HashJoinPointerTable t;
            t.init(
                method,
                row_count,
                hash_value_bytes,
                pointer_table_size > 1 ? pointer_table_size - 1 : 0,
                true,
                true);
            ASSERT_EQ(t.pointer_table_size, pointer_table_size);
            ASSERT_EQ(t.pointer_table_size_degree, pointer_table_size_degree);
            ASSERT_EQ(t.enable_probe_prefetch, true);
            ASSERT_EQ(t.enable_tagged_pointer, true);
            ASSERT_EQ(t.pointer_table_size_mask, pointer_table_size_mask);
        }
        {
            HashJoinPointerTable t;
            t.init(method, row_count, hash_value_bytes, pointer_table_size + 1, false, true);
            ASSERT_EQ(t.pointer_table_size, pointer_table_size);
            ASSERT_EQ(t.pointer_table_size_degree, pointer_table_size_degree);
            ASSERT_EQ(t.enable_probe_prefetch, false);
            ASSERT_EQ(t.enable_tagged_pointer, false);
            ASSERT_EQ(t.pointer_table_size_mask, pointer_table_size_mask);
        }
    }
};

TEST_F(HashJoinPointerTableTest, TestInit)
try
{
    testInit(HashJoinKeyMethod::OneKey8, 100000000, 1, 1 << 8, 8, 0xff);
    testInit(HashJoinKeyMethod::OneKey8, 1, 1, 1 << 8, 8, 0xff);

    testInit(HashJoinKeyMethod::OneKey16, 100000000, 2, 1 << 16, 16, 0xffff);
    testInit(HashJoinKeyMethod::OneKey16, 1, 2, 1 << 16, 16, 0xffff);

    testInit(HashJoinKeyMethod::OneKey32, (1 << 5) + 1, 4, 1 << 10, 10, 0xffc00000);
    testInit(HashJoinKeyMethod::OneKey64, 1 << 24, 4, 1 << 25, 25, 0xffffff80);
    testInit(HashJoinKeyMethod::OneKey128, (1 << 24) + 15, 4, 1 << 26, 26, 0xffffffc0);
    testInit(HashJoinKeyMethod::KeysFixed32, (1 << 26) + 9, 3, 1 << 24, 24, 0xffffff);
    // pointer table size can not exceed 2^32
    testInit(HashJoinKeyMethod::KeysFixed64, (1ULL << 50) + 15, 6, 1ULL << 32, 32, 0xffffffff0000ULL);

    testInit(HashJoinKeyMethod::KeysFixed128, 1 << 4, 8, 1 << 10, 10, 0xffc0000000000000ULL);
    testInit(HashJoinKeyMethod::KeysFixed256, 1 << 17, 8, 1 << 18, 18, 0xffffc00000000000ULL);
    testInit(HashJoinKeyMethod::KeysFixedOther, (1 << 17) + 233, 8, 1 << 19, 19, 0xffffe00000000000ULL);
    testInit(HashJoinKeyMethod::OneKeyString, (1 << 29) + 12345, 8, 1ULL << 31, 31, 0xfffffffe00000000ULL);
    // pointer table size can not exceed 2^32
    testInit(HashJoinKeyMethod::KeySerialized, (1ULL << 60) + 233, 8, 1ULL << 32, 32, 0xffffffff00000000ULL);
}
CATCH

} // namespace tests
} // namespace DB
