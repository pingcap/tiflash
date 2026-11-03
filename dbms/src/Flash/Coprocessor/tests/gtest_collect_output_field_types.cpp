// Copyright 2026 PingCAP, Inc.
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
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/collectOutputFieldTypes.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

#include <array>
#include <limits>

namespace DB::tests
{
namespace
{
tipb::DAGRequest makeExpandRequest(size_t column_count, const String & encoded_index)
{
    tipb::DAGRequest request;
    auto * root = request.mutable_root_executor();
    root->set_tp(tipb::ExecType::TypeExpand);
    auto * expand = root->mutable_expand();
    auto * child = expand->mutable_child();
    child->set_tp(tipb::ExecType::TypeTableScan);
    for (size_t i = 0; i < column_count; ++i)
    {
        auto * column = child->mutable_tbl_scan()->add_columns();
        column->set_tp(TiDB::TypeLongLong);
        column->set_flag(TiDB::ColumnFlagNotNull | TiDB::ColumnFlagUnsigned);
    }
    auto * grouping_col = expand->add_grouping_sets()->add_grouping_exprs()->add_grouping_expr();
    grouping_col->set_tp(tipb::ExprType::ColumnRef);
    grouping_col->set_val(encoded_index);
    return request;
}

String encodeColumnIndex(Int64 index)
{
    WriteBufferFromOwnString buffer;
    encodeDAGInt64(index, buffer);
    return buffer.releaseStr();
}
} // namespace

TEST(CollectOutputFieldTypesTest, ExpandColumnIndexBounds)
{
    for (size_t column_count : {0, 2})
    {
        for (Int64 index :
             std::array<Int64, 7>{-1, 0, 1, 2, 3, std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max()})
        {
            SCOPED_TRACE(fmt::format("column_count={}, index={}", column_count, index));
            const auto request = makeExpandRequest(column_count, encodeColumnIndex(index));
            if (index < 0 || index >= static_cast<Int64>(column_count))
            {
                EXPECT_THROW(collectOutputFieldTypes(request), DB::Exception);
            }
            else
            {
                const auto fields = collectOutputFieldTypes(request);
                ASSERT_EQ(fields.size(), column_count + 1);
                for (size_t i = 0; i < column_count; ++i)
                {
                    EXPECT_EQ(fields[i].tp(), TiDB::TypeLongLong);
                    EXPECT_EQ(
                        fields[i].flag(),
                        TiDB::ColumnFlagUnsigned | (i == static_cast<size_t>(index) ? 0 : TiDB::ColumnFlagNotNull));
                }
                EXPECT_EQ(fields.back().tp(), TiDB::TypeLongLong);
                EXPECT_EQ(fields.back().flag(), TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
            }
        }
    }
}

TEST(CollectOutputFieldTypesTest, ExpandColumnIndexEncoding)
{
    for (size_t length : {0, 1, 7, 9})
    {
        SCOPED_TRACE(length);
        auto encoded_index = encodeColumnIndex(0);
        encoded_index.resize(length, '\0');
        const auto request = makeExpandRequest(2, encoded_index);
        EXPECT_THROW(collectOutputFieldTypes(request), DB::Exception);
    }
}
} // namespace DB::tests
