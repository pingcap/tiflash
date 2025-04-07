// Copyright 2025 PingCAP, Inc.
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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/DeltaMerge/VersionChain/ColumnView.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{
template <ExtraHandleType HandleType>
HandleType genHandle(Int64 i)
{
    if constexpr (isCommonHandle<HandleType>())
        return fmt::format("{:0>3}", i);
    else
        return i;
}

template <ExtraHandleType HandleType>
ColumnPtr genColumn()
{
    MutableColumnPtr col;
    if constexpr (isCommonHandle<HandleType>())
        col = DataTypeFactory::instance().getOrSet(DataTypeString::getDefaultName())->createColumn();
    else
        col = DataTypeFactory::instance().getOrSet("Int64")->createColumn();

    for (Int64 i = 0; i <= 100; i += 2)
    {
        Field f = genHandle<HandleType>(i);
        col->insert(f);
    }
    return col;
}

template <ExtraHandleType HandleType>
void testColumnView()
{
    auto col = genColumn<HandleType>();
    ColumnView<HandleType> cv(*col);
    ASSERT_TRUE(std::is_sorted(cv.begin(), cv.end())) << fmt::format("{}", cv);
    for (auto itr = cv.begin(); itr != cv.end(); ++itr)
    {
        Field f1 = HandleType{*itr};
        Field f2 = HandleType{cv[itr - cv.begin()]};
        Field f3 = (*col)[itr - cv.begin()];
        ASSERT_EQ(f1, f2);
        ASSERT_EQ(f2, f3);

        ASSERT_EQ(std::find(cv.begin(), cv.end(), *itr), itr);
        ASSERT_EQ(std::lower_bound(cv.begin(), cv.end(), *itr), itr);
    }

    for (Int64 i = 0; i <= 100; ++i)
    {
        const auto h = genHandle<HandleType>(i);
        const auto itr1 = std::find(cv.begin(), cv.end(), h);
        const auto itr2 = std::lower_bound(cv.begin(), cv.end(), h);
        if (i % 2 == 0)
        {
            ASSERT_NE(itr1, cv.end());
            ASSERT_EQ(itr1, itr2);
        }
        else
        {
            ASSERT_EQ(itr1, cv.end());
            ASSERT_NE(itr2, cv.end());
            ASSERT_EQ(itr2, std::lower_bound(cv.begin(), cv.end(), genHandle<HandleType>(i + 1)));
        }
    }
}

TEST(ColumnView, String)
{
    testColumnView<String>();
}

TEST(ColumnView, Int64)
{
    testColumnView<Int64>();
}
} // namespace DB::DM::tests
