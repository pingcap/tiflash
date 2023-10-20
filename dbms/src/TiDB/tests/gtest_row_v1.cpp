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

#include <TiDB/tests/RowCodecTestUtils.h>
#include <gtest/gtest.h>

namespace DB::tests
{
#define ASSERT_INT_VALUE(v) ASSERT_EQ(getValueByRowV1(v), v)

TEST(RowV1Suite, IntValue)
{
    ASSERT_INT_VALUE(std::numeric_limits<Int8>::max());
    ASSERT_INT_VALUE(std::numeric_limits<Int8>::min());
    ASSERT_INT_VALUE(std::numeric_limits<UInt8>::max());
    ASSERT_INT_VALUE(std::numeric_limits<UInt8>::min());
    ASSERT_INT_VALUE(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::max()) + Int16(1)));
    ASSERT_INT_VALUE(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::min()) - Int16(1)));
    ASSERT_INT_VALUE(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::max()) + UInt16(1)));
    ASSERT_INT_VALUE(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::min()) - UInt16(1)));
    ASSERT_INT_VALUE(std::numeric_limits<Int16>::max());
    ASSERT_INT_VALUE(std::numeric_limits<Int16>::min());
    ASSERT_INT_VALUE(std::numeric_limits<UInt16>::max());
    ASSERT_INT_VALUE(std::numeric_limits<UInt16>::min());
    ASSERT_INT_VALUE(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::max()) + Int32(1)));
    ASSERT_INT_VALUE(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::min()) - Int32(1)));
    ASSERT_INT_VALUE(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::max()) + UInt32(1)));
    ASSERT_INT_VALUE(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::min()) - UInt32(1)));
    ASSERT_INT_VALUE(std::numeric_limits<Int32>::max());
    ASSERT_INT_VALUE(std::numeric_limits<Int32>::min());
    ASSERT_INT_VALUE(std::numeric_limits<UInt32>::max());
    ASSERT_INT_VALUE(std::numeric_limits<UInt32>::min());
    ASSERT_INT_VALUE(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::max()) + Int64(1)));
    ASSERT_INT_VALUE(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::min()) - Int64(1)));
    ASSERT_INT_VALUE(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + UInt64(1)));
    ASSERT_INT_VALUE(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::min()) - UInt64(1)));
    ASSERT_INT_VALUE(std::numeric_limits<Int64>::max());
    ASSERT_INT_VALUE(std::numeric_limits<Int64>::min());
    ASSERT_INT_VALUE(std::numeric_limits<UInt64>::max());
    ASSERT_INT_VALUE(std::numeric_limits<UInt64>::min());
}

#define ASSERT_FLOAT_VALUE(f) ASSERT_DOUBLE_EQ(getValueByRowV1(f), f)

TEST(RowV1Suite, FloatValue)
{
    ASSERT_FLOAT_VALUE(Float32(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::max());
    ASSERT_FLOAT_VALUE(Float64(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::max());
}

#define ASSERT_STRING_VALUE(b, s) ASSERT_EQ(getValueByRowV1(s), s)

TEST(RowV1Suite, StringValue)
{
    ASSERT_STRING_VALUE(false, String(""));
    ASSERT_STRING_VALUE(false, String("aaa"));
    ASSERT_STRING_VALUE(false, String(std::numeric_limits<UInt16>::max(), 'a'));
    ASSERT_STRING_VALUE(true, String(std::numeric_limits<UInt16>::max() + 1, 'a'));
}

#define ASSERT_DECIMAL_VALUE(d) ASSERT_EQ(getValueByRowV1(d), d)

TEST(RowV1Suite, DecimalValue)
{
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4));
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5));
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 2), 2));
}

} // namespace DB::tests
