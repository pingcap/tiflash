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

#include <Common/BitpackingPrimitives.h>
#include <TestUtils/TiFlashTestBasic.h>


namespace DB::tests
{

class TestBitpackingPrimitives : public testing::Test
{
};

TEST_F(TestBitpackingPrimitives, TestMinimumBitWidthSingle)
try
{
    {
        const UInt8 value = 0b0000'0000;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 0);
    }
    {
        const UInt8 value = 0b0000'0001;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 1);
    }
    {
        const UInt8 value = 0b0000'0010;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 2);
    }
    {
        const UInt16 value = 0b0000'0010'0000'0100;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 10);
    }
    {
        const UInt32 value = 0b0000'0010'0000'0100'0000'0100'0000'0100;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 26);
    }
    {
        const UInt64 value = 0b1000'0010'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 64);
    }
    {
        const Int8 value = -1; // (0b1000'0001 >> 7) ^ (0b1000'0001 << 1) = 0b0000'0010
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 2);
    }
    {
        const Int16 value = -3; // (0b1000'0000'0000'0011 >> 15) ^ (0b1000'0000'0000'0011 << 1) = 0b1000'0000'0000'0111
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 3);
    }
    {
        const Int32 value = -7; // just like above
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 4);
    }
    {
        const Int64 value = 10; // just like above
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(value), 5);
    }
}
CATCH

TEST_F(TestBitpackingPrimitives, TestMinimumBitWidthMinMax)
try
{
    {
        const UInt8 min = 0b0000'0000;
        const UInt8 max = 0b0000'0001;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 1);
    }
    {
        const UInt16 min = 0b0000'0000'0000'0100;
        const UInt16 max = 0b1000'0000'0000'0100;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 16);
    }
    {
        const UInt32 min = 0b0000'0000'0000'0101'0000'0100'0000'0100;
        const UInt32 max = 0b1000'0101'0000'0100'0000'0100'0000'0100;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 32);
    }
    {
        const UInt64 min = 0b0000'0010'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100;
        const UInt64 max = 0b1000'0010'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 64);
    }
    {
        const Int8 min = -1;
        const Int8 max = 10;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 5);
    }
    {
        const Int16 min = -7;
        const Int16 max = 10;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 5);
    }
    {
        const Int32 min = 8;
        const Int32 max = 10;
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 5);
    }
    {
        const Int64 min = std::numeric_limits<Int64>::min();
        const Int64 max = std::numeric_limits<Int64>::max();
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(min, max), 64);
    }
}
CATCH

TEST_F(TestBitpackingPrimitives, TestMinimumBitWidthMultiple)
try
{
    {
        const UInt8 values[3] = {0b0000'0000, 0b0000'0001, 0b0000'0010};
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 2);
    }
    {
        const UInt16 values[3] = {0b0000'0010'0000'0100, 0b0000'0010'1000'0100, 0b0000'0000'0000'0100};
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 10);
    }
    {
        const UInt32 values[3] = {
            0b0000'0010'0000'0100'0000'0100'0000'0100,
            0b0000'0011'0000'0100'0000'0100'0000'0100,
            0b0000'0000'0000'0100'0000'0100'0000'0100,
        };
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 26);
    }
    {
        const UInt64 values[3] = {
            0b1000'0010'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100,
            0b0000'0011'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100,
            0b0000'0000'0000'0100'0000'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000,
        };
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 64);
    }
    {
        const Int8 values[3] = {-1, -7, 10};
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 5);
    }
    {
        const Int16 values[3] = {-1, -7, 10};
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 5);
    }
    {
        const Int32 values[3] = {-1, -7, 10};
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 5);
    }
    {
        const Int64 values[3] = {-1, -7, std::numeric_limits<Int64>::max()};
        ASSERT_EQ(BitpackingPrimitives::minimumBitWidth(values, 3), 64);
    }
}
CATCH


TEST_F(TestBitpackingPrimitives, TestGetRequiredSize)
try
{
    // 32 as a group
    ASSERT_EQ(BitpackingPrimitives::getRequiredSize(1, 8), 32);
    ASSERT_EQ(BitpackingPrimitives::getRequiredSize(32, 8), 32);
    ASSERT_EQ(BitpackingPrimitives::getRequiredSize(2, 7), 28);
    ASSERT_EQ(BitpackingPrimitives::getRequiredSize(32, 7), 28);
    ASSERT_EQ(BitpackingPrimitives::getRequiredSize(2, 10), 40);
    ASSERT_EQ(BitpackingPrimitives::getRequiredSize(32, 10), 40);
}
CATCH

TEST_F(TestBitpackingPrimitives, TestpackBuffer)
try
{
    auto test_func = []<typename T>(const T * values, size_t size) {
        const auto width = BitpackingPrimitives::minimumBitWidth(values, size);
        const auto length = BitpackingPrimitives::getRequiredSize(size, width);
        unsigned char buffer[length];
        BitpackingPrimitives::packBuffer(buffer, values, size, width);
        // dest buffer should be rounded up to group size
        const auto round_count = BitpackingPrimitives::roundUpToAlgorithmGroupSize(size);
        unsigned char decoded[sizeof(T) * round_count];
        BitpackingPrimitives::unPackBuffer<T>(decoded, buffer, size, width);
        for (size_t i = 0; i < size; ++i)
        {
            auto decode_value = unalignedLoad<T>(decoded + i * sizeof(T));
            ASSERT_EQ(decode_value, values[i]);
        }
    };

    {
        const UInt8 values[3] = {0b0000'0000, 0b0000'0001, 0b0000'0010};
        test_func(values, 3);
    }
    {
        const UInt16 values[3] = {0b0000'0010'0000'0100, 0b0000'0010'1000'0100, 0b0000'0000'0000'0100};
        test_func(values, 3);
    }
    {
        const UInt32 values[3] = {
            0b0000'0010'0000'0100'0000'0100'0000'0100,
            0b0000'0011'0000'0100'0000'0100'0000'0100,
            0b0000'0000'0000'0100'0000'0100'0000'0100,
        };
        test_func(values, 3);
    }
    {
        const UInt64 values[3] = {
            0b1000'0010'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100,
            0b0000'0011'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100,
            0b0000'0000'0000'0100'0000'0000'0100'0000'0100'0000'0100'0000'0100'0000'0100'0000,
        };
        test_func(values, 3);
    }
    {
        const Int8 values[3] = {-1, -7, 10};
        test_func(values, 3);
    }
    {
        const Int16 values[3] = {-1, -7, 10};
        test_func(values, 3);
    }
    {
        const Int32 values[3] = {-1, -7, 10};
        test_func(values, 3);
    }
    {
        const Int64 values[3] = {-1, -7, std::numeric_limits<Int64>::max()};
        test_func(values, 3);
    }
}
CATCH

} // namespace DB::tests
