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

#include <Common/Exception.h>
#include <Common/VectorWriter.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <tuple>

namespace DB
{
namespace tests
{
class TestVectorWriter : public testing::Test
{
};

TEST_F(TestVectorWriter, test)
try
{
    PaddedPODArray<UInt8> vector;
    {
        VectorWriter writer(vector, 1);
        ASSERT_EQ(writer.offset(), 0);

        writer.write('a');
        ASSERT_EQ(writer.offset(), 1);

        writer.advance(3);
        ASSERT_EQ(writer.offset(), 4);

        PaddedPODArray<UInt8> tmp;
        tmp.resize_fill(5, 'a');
        writer.write(reinterpret_cast<char *>(tmp.data()), tmp.size());
        ASSERT_EQ(writer.offset(), 9);

        writer.setOffset(1);
        writer.write('a');
        writer.write('b');
        writer.write('c');
        writer.setOffset(9);
    }
    ASSERT_EQ(vector.size(), 9);
    StringRef str(reinterpret_cast<char *>(vector.data()), vector.size());
    ASSERT_EQ(str, "aabcaaaaa");
}
CATCH

TEST_F(TestVectorWriter, allocFirst)
try
{
    PaddedPODArray<UInt8> vector;
    {
        VectorWriter writer(vector, 1);
        ASSERT_EQ(writer.offset(), 0);

        writer.advance(3);
        ASSERT_EQ(writer.offset(), 3);

        writer.write('a');
        ASSERT_EQ(writer.offset(), 4);

        PaddedPODArray<UInt8> tmp;
        tmp.resize_fill(5, 'a');
        writer.write(reinterpret_cast<char *>(tmp.data()), tmp.size());
        ASSERT_EQ(writer.offset(), 9);

        writer.setOffset(0);
        writer.write('a');
        writer.write('b');
        writer.write('c');
        writer.setOffset(9);
    }
    ASSERT_EQ(vector.size(), 9);
    StringRef str(reinterpret_cast<char *>(vector.data()), vector.size());
    ASSERT_EQ(str, "abcaaaaaa");
}
CATCH

} // namespace tests
} // namespace DB
