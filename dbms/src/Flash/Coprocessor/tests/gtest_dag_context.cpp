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

#include <Common/Decimal.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>
#include <kvproto/disaggregated.pb.h>

#include <array>
#include <limits>
#include <memory>

namespace DB
{
namespace tests
{
namespace
{
tipb::DAGRequest makeDecimalRequest()
{
    tipb::DAGRequest request;
    auto * root = request.mutable_root_executor();
    root->set_tp(tipb::ExecType::TypeTableScan);
    root->set_executor_id("scan");
    auto * column = root->mutable_tbl_scan()->add_columns();
    column->set_tp(TiDB::TypeNewDecimal);
    column->set_columnlen(10);
    column->set_decimal(2);
    request.add_output_offsets(0);
    request.set_encode_type(tipb::EncodeType::TypeCHBlock);
    return request;
}

template <typename Check>
void checkContextConstructors(tipb::DAGRequest & request, Check && check)
{
    const auto log = Logger::get("DAGContextTest");
    check([&] {
        return std::make_unique<
            DAGContext>(request, TablesRegionsInfo{}, NullspaceID, "", DAGRequestKind::Cop, "", 0, "", log);
    });
    check([&] { return std::make_unique<DAGContext>(request, mpp::TaskMeta{}, false); });
    check([&] {
        return std::make_unique<DAGContext>(request, disaggregated::DisaggTaskMeta{}, TablesRegionsInfo{}, "", log);
    });
    check([&] { return std::make_unique<DAGContext>(request, "DAGContextTest", 1); });
}
} // namespace

TEST(DAGContextTest, DivPrecisionIncrementDefaultAndBoundaries)
{
    auto request = makeDecimalRequest();
    checkContextConstructors(request, [](auto && make_context) {
        const auto context = make_context();
        EXPECT_EQ(context->getDivPrecisionIncrement(), DEFAULT_DIV_PRECISION_INCREMENT);
    });

    for (const Int32 increment :
         std::array<Int32, 3>{0, DEFAULT_DIV_PRECISION_INCREMENT, static_cast<Int32>(decimal_max_scale)})
    {
        SCOPED_TRACE(increment);
        request.set_div_precision_increment(increment);
        checkContextConstructors(request, [&](auto && make_context) {
            const auto context = make_context();
            EXPECT_EQ(context->getDivPrecisionIncrement(), increment);
        });
    }
}

TEST(DAGContextTest, RejectsInvalidDivPrecisionIncrement)
{
    auto request = makeDecimalRequest();
    for (const Int32 increment :
         std::array<Int32, 4>{std::numeric_limits<Int32>::min(), -1, 31, std::numeric_limits<Int32>::max()})
    {
        SCOPED_TRACE(increment);
        request.set_div_precision_increment(increment);
        checkContextConstructors(request, [](auto && make_context) {
            try
            {
                make_context();
                FAIL() << "Expected an invalid division precision increment to be rejected";
            }
            catch (const TiFlashException & e)
            {
                EXPECT_TRUE(e.getError().is(Errors::Coprocessor::BadRequest)) << e.standardText();
                EXPECT_NE(e.standardText().find("div_precision_increment"), String::npos) << e.standardText();
            }
        });
    }
}

TEST(DAGContextTest, FlagsTest)
{
    DAGContext context(1024);

    ASSERT_EQ(context.getFlags(), static_cast<UInt64>(0));

    UInt64 f = TiDBSQLFlags::TRUNCATE_AS_WARNING | TiDBSQLFlags::IN_LOAD_DATA_STMT;
    context.setFlags(f);
    ASSERT_EQ(context.getFlags(), f);

    UInt64 f1 = f | TiDBSQLFlags::TRUNCATE_AS_WARNING;
    context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    ASSERT_EQ(context.getFlags(), f1);

    context.delFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    ASSERT_EQ(context.getFlags(), TiDBSQLFlags::IN_LOAD_DATA_STMT);
}

} // namespace tests
} // namespace DB
