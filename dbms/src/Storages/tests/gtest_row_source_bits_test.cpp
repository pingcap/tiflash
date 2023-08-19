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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop

#include <DataStreams/ColumnGathererStream.h>
using DB::RowSourcePart;

static void check(const RowSourcePart & s, size_t num, bool flag)
{
    EXPECT_FALSE((s.getSourceNum() != num || s.getSkipFlag() != flag) || (!flag && s.data != num));
}

TEST(ColumnGathererStream, RowSourcePartBitsTest)
{
    check(RowSourcePart(0, false), 0, false);
    check(RowSourcePart(0, true), 0, true);
    check(RowSourcePart(1, false), 1, false);
    check(RowSourcePart(1, true), 1, true);
    check(RowSourcePart(RowSourcePart::MAX_PARTS, false), RowSourcePart::MAX_PARTS, false);
    check(RowSourcePart(RowSourcePart::MAX_PARTS, true), RowSourcePart::MAX_PARTS, true);

    RowSourcePart p{80, false};
    check(p, 80, false);
    p.setSkipFlag(true);
    check(p, 80, true);
    p.setSkipFlag(false);
    check(p, 80, false);
    p.setSourceNum(RowSourcePart::MAX_PARTS);
    check(p, RowSourcePart::MAX_PARTS, false);
}
