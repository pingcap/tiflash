#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/registerFunctions.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>
#include <tipb/executor.pb.h>

namespace DB::tests
{
class HiddenCommitTSColumnTest : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }

protected:
    LoggerPtr log = Logger::get();
    ContextPtr ctx = TiFlashTestEnv::getContext();
};

TEST_F(HiddenCommitTSColumnTest, PushDownFilterAliasAndCast)
try
{
    // TiDB may request a hidden column with ColumnID=-5 (commit_ts). In TiFlash storage layer it is stored in
    // `_INTERNAL_VERSION` with ColumnID=VersionColumnID. When TiDB column type differs (e.g. Nullable(Int64)),
    // TiFlash should add a cast.

    TiDB::ColumnInfo commit_ts_ci;
    commit_ts_ci.id = ExtraCommitTSColumnID;
    commit_ts_ci.name = "commit_ts";
    commit_ts_ci.tp = TiDB::TypeLongLong; // Int64
    commit_ts_ci.flag = 0; // Nullable(Int64)

    TiDB::ColumnInfos table_scan_column_info{commit_ts_ci};

    // Use a single ColumnRef as filter condition: "where commit_ts".
    // This is enough to trigger:
    // 1) filter column id extraction (ColumnID=-5)
    // 2) aliasing from -5 to VersionColumnID in PushDownFilter
    // 3) extra cast generation based on ColumnInfo (Nullable(Int64)) vs storage type (VERSION_COLUMN_TYPE)
    google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters;
    {
        auto * cond = pushed_down_filters.Add();
        cond->set_tp(tipb::ExprType::ColumnRef);
        {
            WriteBufferFromOwnString ss;
            encodeDAGInt64(/*column_index=*/0, ss);
            cond->set_val(ss.releaseStr());
        }
        auto * field_type = cond->mutable_field_type();
        field_type->set_tp(TiDB::TypeLongLong);
        field_type->set_flag(0); // Nullable
        field_type->set_flen(0);
        field_type->set_decimal(0);
    }

    DM::ColumnDefines columns_to_read;
    columns_to_read.emplace_back(VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);

    auto filter = DM::PushDownFilter::build(
        DM::EMPTY_RS_OPERATOR,
        table_scan_column_info,
        pushed_down_filters,
        columns_to_read,
        *ctx,
        log);

    ASSERT_TRUE(filter);
    ASSERT_TRUE(filter->filter_columns);
    ASSERT_EQ(filter->filter_columns->size(), 1);
    // Storage must read VersionColumnID, not -5.
    EXPECT_EQ(filter->filter_columns->at(0).id, VERSION_COLUMN_ID);
    EXPECT_EQ(filter->filter_columns->at(0).name, VERSION_COLUMN_NAME);

    // Extra cast should exist because TiDB requires Nullable(Int64) while TiFlash storage uses VERSION_COLUMN_TYPE.
    ASSERT_TRUE(filter->extra_cast);

    Block block = Block{
        {toVec<UInt64>(VERSION_COLUMN_NAME, {1, 2, 3, 4})},
    };
    filter->extra_cast->execute(block);

    const auto expected_type = getDataTypeByColumnInfoForComputingLayer(commit_ts_ci);
    ASSERT_TRUE(block.has(VERSION_COLUMN_NAME));
    EXPECT_EQ(block.getByName(VERSION_COLUMN_NAME).type->getName(), expected_type->getName());
}
CATCH

TEST_F(HiddenCommitTSColumnTest, CastAfterTableScanForCommitTS)
try
{
    // Non-late-materialization path:
    // TiDB may request a hidden column with ColumnID=-5 (commit_ts). In TiFlash storage layer it is stored in
    // `_INTERNAL_VERSION` with type VERSION_COLUMN_TYPE (currently UInt64). If TiDB column type differs
    // (e.g. Nullable(Int64)), TiFlash should add a cast after TableScan and keep the output column name unchanged.

    TiDB::ColumnInfo commit_ts_ci;
    commit_ts_ci.id = ExtraCommitTSColumnID;
    commit_ts_ci.name = "commit_ts";
    commit_ts_ci.tp = TiDB::TypeLongLong; // Int64
    commit_ts_ci.flag = 0; // Nullable

    TiDB::ColumnInfos table_scan_column_info{commit_ts_ci};
    std::vector<UInt8> may_need_add_cast_column{/*commit_ts*/ 1};

    Block block = Block{
        {toVec<UInt64>(VERSION_COLUMN_NAME, {1, 2, 3, 4})},
    };

    DAGExpressionAnalyzer analyzer{block, *ctx};
    ExpressionActionsChain chain;
    auto & step = analyzer.initAndGetLastStep(chain);
    auto & actions = step.actions;

    auto [has_cast, casted_columns]
        = analyzer.buildExtraCastsAfterTS(actions, may_need_add_cast_column, table_scan_column_info);
    ASSERT_TRUE(has_cast);
    ASSERT_EQ(casted_columns.size(), 1);

    // Mimic appendExtraCastsAfterTS: project casted columns back to original names.
    NamesWithAliases project_cols;
    project_cols.emplace_back(casted_columns[0], VERSION_COLUMN_NAME);
    actions->add(ExpressionAction::project(project_cols));
    step.required_output.push_back(VERSION_COLUMN_NAME);

    ExpressionActionsPtr extra_cast = chain.getLastActions();
    ASSERT_TRUE(extra_cast);
    chain.finalize();
    chain.clear();

    extra_cast->execute(block);

    const auto expected_type = getDataTypeByColumnInfoForComputingLayer(commit_ts_ci);
    ASSERT_TRUE(block.has(VERSION_COLUMN_NAME));
    EXPECT_EQ(block.getByName(VERSION_COLUMN_NAME).type->getName(), expected_type->getName());
}
CATCH

TEST_F(HiddenCommitTSColumnTest, RoughSetFilterAliasCommitTS)
try
{
    // Rough set filter (RSOperator) uses table_column_defines by ColumnID.
    // TiDB requests commit_ts as ColumnID=-5, but in TiFlash it is stored in `_INTERNAL_VERSION` (VersionColumnID).
    // Ensure rough set filter can correctly map ColumnID=-5 to VersionColumnID.

    TiDB::ColumnInfo commit_ts_ci;
    commit_ts_ci.id = ExtraCommitTSColumnID;
    commit_ts_ci.name = "commit_ts";
    commit_ts_ci.tp = TiDB::TypeLongLong; // Int64
    commit_ts_ci.flag = 0; // Nullable
    TiDB::ColumnInfos scan_column_infos{commit_ts_ci};

    google::protobuf::RepeatedPtrField<tipb::Expr> filters;
    {
        tipb::Expr col_ref;
        col_ref.set_tp(tipb::ExprType::ColumnRef);
        {
            WriteBufferFromOwnString ss;
            encodeDAGInt64(/*column_index=*/0, ss);
            col_ref.set_val(ss.releaseStr());
        }
        auto * field_type = col_ref.mutable_field_type();
        field_type->set_tp(TiDB::TypeLongLong);
        field_type->set_flag(0); // Nullable
        field_type->set_flen(0);
        field_type->set_decimal(0);

        tipb::Expr literal = constructInt64LiteralTiExpr(10);

        auto * func = filters.Add();
        func->set_tp(tipb::ExprType::ScalarFunc);
        func->set_sig(tipb::ScalarFuncSig::GTInt);
        *func->add_children() = col_ref;
        *func->add_children() = literal;
    }

    tipb::ANNQueryInfo ann_query_info;
    google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters;
    std::vector<int> runtime_filter_ids;
    const int rf_max_wait_time_ms = 0;
    auto dag_query = std::make_unique<DAGQueryInfo>(
        filters,
        ann_query_info,
        pushed_down_filters,
        scan_column_infos,
        runtime_filter_ids,
        rf_max_wait_time_ms,
        ctx->getTimezoneInfo());

    DM::ColumnDefines table_column_defines;
    table_column_defines.emplace_back(VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);

    auto rs_operator
        = DM::RSOperator::build(dag_query, scan_column_infos, table_column_defines, /*enable_rs_filter*/ true, log);
    ASSERT_TRUE(rs_operator);

    const auto col_ids = rs_operator->getColumnIDs();
    ASSERT_EQ(col_ids.size(), 1);
    EXPECT_EQ(col_ids[0], VERSION_COLUMN_ID);
}
CATCH

} // namespace DB::tests
