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

#include <DataStreams/BlocksListBlockInputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Functions/registerFunctions.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/PushDownExecutor.h>
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

TEST_F(HiddenCommitTSColumnTest, DisaggregatedReadCommitTS)
try
{
    for (bool partition_scan : {false, true})
    {
        for (UInt32 flags : {static_cast<UInt32>(0), static_cast<UInt32>(TiDB::ColumnFlagNotNull | TiDB::ColumnFlagUnsigned)})
        {
            SCOPED_TRACE(fmt::format("partition_scan={} flags={}", partition_scan, flags));
            tipb::Executor executor;
            executor.set_tp(partition_scan ? tipb::TypePartitionTableScan : tipb::TypeTableScan);
            auto * columns = partition_scan ? executor.mutable_partition_table_scan()->mutable_columns()
                                            : executor.mutable_tbl_scan()->mutable_columns();
            if (partition_scan)
                executor.mutable_partition_table_scan()->set_table_id(100);
            else
                executor.mutable_tbl_scan()->set_table_id(100);
            auto * handle = columns->Add();
            handle->set_column_id(MutSup::extra_handle_id);
            handle->set_tp(TiDB::TypeLongLong);
            handle->set_flag(TiDB::ColumnFlagNotNull);
            auto * commit_ts = columns->Add();
            commit_ts->set_column_id(MutSup::extra_commit_ts_col_id);
            commit_ts->set_tp(TiDB::TypeLongLong);
            commit_ts->set_flag(flags);
            auto * table_id = columns->Add();
            table_id->set_column_id(MutSup::extra_table_id_col_id);
            table_id->set_tp(TiDB::TypeLongLong);
            table_id->set_flag(TiDB::ColumnFlagNotNull);

            DAGContext dag_context(1024);
            TiDBTableScan table_scan(&executor, "table_scan", dag_context);
            auto [column_defines, extra_table_id_index, generated_columns]
                = genColumnDefinesForDisaggregatedRead(table_scan);
            ASSERT_EQ(column_defines->size(), 2);
            EXPECT_EQ(extra_table_id_index, 2);
            EXPECT_TRUE(generated_columns.empty());
            const auto & version = column_defines->at(1);
            EXPECT_EQ(version.id, MutSup::version_col_id);
            EXPECT_EQ(version.name, genNameForExchangeReceiver(1));
            EXPECT_TRUE(version.type->equals(*MutSup::getVersionColumnType()));

            // Both normal scans and late materialization must preserve the real
            // version values while converting the exchange receiver column type.
            const auto & commit_ts_info = table_scan.getColumns()[1];
            const auto expected_type = getDataTypeByColumnInfoForComputingLayer(commit_ts_info);
            for (bool pushed_down : {false, true})
            {
                SCOPED_TRACE(pushed_down);
                Block stored_block{
                    toVec<Int64>(MutSup::extra_handle_column_name, {1, 1, 2, 3, 3, 4}),
                    toVec<UInt64>(MutSup::version_column_name, {0, 1, 123456789, 456789012, 999999999, 1}),
                    toVec<UInt8>(MutSup::delmark_column_name, {0, 0, 0, 0, 0, 1}),
                };
                stored_block.getByPosition(0).column_id = MutSup::extra_handle_id;
                stored_block.getByPosition(1).column_id = MutSup::version_col_id;
                stored_block.getByPosition(2).column_id = MutSup::delmark_col_id;
                auto input = std::make_shared<BlocksListBlockInputStream>(BlocksList{stored_block});
                DM::DMVersionFilterBlockInputStream<DM::DMVersionFilterMode::MVCC> stream(
                    input,
                    {version},
                    /*version_limit=*/500000000,
                    /*is_common_handle=*/false);
                stream.readPrefix();
                auto block = stream.read();
                ASSERT_FALSE(stream.read());
                stream.readSuffix();
                ExpressionActionsPtr cast;
                if (pushed_down)
                {
                    google::protobuf::RepeatedPtrField<tipb::Expr> filters;
                    auto * condition = filters.Add();
                    condition->set_tp(tipb::ExprType::ColumnRef);
                    WriteBufferFromOwnString buffer;
                    encodeDAGInt64(0, buffer);
                    condition->set_val(buffer.releaseStr());
                    condition->mutable_field_type()->set_tp(TiDB::TypeLongLong);
                    condition->mutable_field_type()->set_flag(flags);
                    auto push_down = DM::PushDownExecutor::build(
                        DM::EMPTY_RS_OPERATOR,
                        nullptr,
#if ENABLE_CLARA
                        nullptr,
#endif
                        {commit_ts_info},
                        filters,
                        {version},
                        nullptr,
                        *ctx,
                        log);
                    ASSERT_TRUE(push_down->filter_columns);
                    ASSERT_EQ(push_down->filter_columns->size(), 1);
                    EXPECT_EQ(push_down->filter_columns->at(0).id, MutSup::version_col_id);
                    EXPECT_EQ(push_down->filter_columns->at(0).name, version.name);
                    cast = push_down->extra_cast;
                }
                else
                {
                    DAGExpressionAnalyzer analyzer{block, *ctx};
                    ExpressionActionsChain chain;
                    auto & step = analyzer.initAndGetLastStep(chain);
                    auto [has_cast, casted_columns]
                        = analyzer.buildExtraCastsAfterTS(step.actions, {1}, {commit_ts_info});
                    if (has_cast)
                    {
                        step.actions->add(
                            ExpressionAction::project(NamesWithAliases{{casted_columns[0], version.name}}));
                        step.required_output.push_back(version.name);
                        cast = chain.getLastActions();
                        chain.finalize();
                    }
                }
                EXPECT_EQ(bool(cast), !version.type->equals(*expected_type));
                if (cast)
                    cast->execute(block);
                const auto & result = block.getByName(version.name);
                EXPECT_TRUE(result.type->equals(*expected_type));
                ASSERT_EQ(result.column->size(), 3);
                for (size_t i = 0; i < 3; ++i)
                {
                    const auto value = (*result.column)[i];
                    const Int64 actual = flags == 0 ? value.safeGet<Int64>() : value.safeGet<UInt64>();
                    EXPECT_EQ(actual, (std::vector<Int64>{1, 123456789, 456789012})[i]);
                }
            }
        }
    }
}
CATCH

TEST_F(HiddenCommitTSColumnTest, ColumnarCommitTSRequestAndWireFormat)
try
{
    for (bool partition_scan : {false, true})
    {
        for (UInt32 flags :
             {static_cast<UInt32>(0),
              static_cast<UInt32>(TiDB::ColumnFlagUnsigned),
              static_cast<UInt32>(TiDB::ColumnFlagNotNull),
              static_cast<UInt32>(TiDB::ColumnFlagNotNull | TiDB::ColumnFlagUnsigned)})
        {
            SCOPED_TRACE(fmt::format("partition_scan={} flags={}", partition_scan, flags));
            tipb::Executor executor;
            executor.set_tp(partition_scan ? tipb::TypePartitionTableScan : tipb::TypeTableScan);
            auto * columns = partition_scan ? executor.mutable_partition_table_scan()->mutable_columns()
                                            : executor.mutable_tbl_scan()->mutable_columns();
            if (partition_scan)
                executor.mutable_partition_table_scan()->set_table_id(100);
            else
                executor.mutable_tbl_scan()->set_table_id(100);
            auto * commit_ts = columns->Add();
            commit_ts->set_column_id(MutSup::extra_commit_ts_col_id);
            commit_ts->set_tp(TiDB::TypeLongLong);
            commit_ts->set_flag(flags);
            DAGContext dag_context(1024);
            TiDBTableScan scan(&executor, "scan", dag_context);
            auto [defines, table_id_index] = genColumnDefinesForDisaggregatedReadThroughColumnar(scan);
            ASSERT_EQ(defines->size(), 1);
            EXPECT_EQ(table_id_index, MutSup::invalid_col_id);
            const auto & version = defines->front();
            EXPECT_EQ(version.id, MutSup::version_col_id);
            EXPECT_EQ(version.name, genNameForExchangeReceiver(0));
            EXPECT_EQ(version.type->getName(), "Nullable(UInt64)");

            const auto table_info = genTableInfoForColumnarRead(scan);
            // A version-only projection must still request real MVCC buffers.
            // Requesting the handle disables CSE's dummy-version pack clean read.
            ASSERT_EQ(table_info.columns_size(), 1);
            EXPECT_EQ(table_info.columns(0).column_id(), MutSup::extra_handle_id);
            const auto storage_scan = genTableScanForColumnarRead(scan);
            const auto & storage_columns
                = partition_scan ? storage_scan.partition_table_scan().columns() : storage_scan.tbl_scan().columns();
            ASSERT_EQ(storage_columns.size(), 1);
            EXPECT_EQ(storage_columns[0].column_id(), MutSup::version_col_id);
            EXPECT_EQ(scan.getColumns()[0].id, MutSup::extra_commit_ts_col_id);
            EXPECT_EQ(getStorageColumnIDForColumnarRead(MutSup::extra_commit_ts_col_id), version.id);
            EXPECT_EQ(getStorageColumnIDForColumnarRead(42), 42);

            // ffi_read_version serializes a null map followed by UInt64 values.
            // Visible versions are never null, but their null-map bytes remain.
            const std::vector<UInt64> timestamps{1, 123456789, 456789012};
            WriteBufferFromOwnString output;
            for (size_t i = 0; i < timestamps.size(); ++i)
                writeBinary(static_cast<UInt32>(0), output);
            for (auto ts : timestamps)
                writeBinary(ts, output);
            const auto bytes = output.releaseStr();
            ReadBufferFromMemory input(bytes.data(), bytes.size());
            auto column = version.type->createColumn();
            version.type->deserializeBinaryBulkWithMultipleStreams(
                *column,
                [&](const IDataType::SubstreamPath &) { return &input; },
                timestamps.size(),
                -1.0,
                true,
                {});
            EXPECT_TRUE(input.eof());
            Block block{{std::move(column), version.type, version.name, version.id}};
            DAGExpressionAnalyzer analyzer{block, *ctx};
            ExpressionActionsChain chain;
            if (analyzer.appendExtraCastsAfterTS(chain, {1}, scan))
            {
                auto actions = chain.getLastActions();
                chain.finalize();
                actions->execute(block);
            }
            const auto & result = block.getByName(version.name);
            EXPECT_TRUE(result.type->equals(*getDataTypeByColumnInfoForComputingLayer(scan.getColumns()[0])));
            ASSERT_EQ(result.column->size(), timestamps.size());
            for (size_t i = 0; i < timestamps.size(); ++i)
            {
                const auto value = (*result.column)[i];
                const UInt64 actual
                    = flags & TiDB::ColumnFlagUnsigned ? value.safeGet<UInt64>() : value.safeGet<Int64>();
                EXPECT_EQ(actual, timestamps[i]);
            }
        }
    }
}
CATCH

TEST_F(HiddenCommitTSColumnTest, ColumnarFilterRemappingUsesStorageProjectionBeforeCast)
try
{
    for (UInt32 flags : {static_cast<UInt32>(0), static_cast<UInt32>(TiDB::ColumnFlagNotNull | TiDB::ColumnFlagUnsigned)})
    {
        SCOPED_TRACE(flags);
        TiDB::ColumnInfo business;
        business.id = 10;
        business.tp = TiDB::TypeLongLong;
        TiDB::ColumnInfo commit_ts;
        commit_ts.id = MutSup::extra_commit_ts_col_id;
        commit_ts.tp = TiDB::TypeLongLong;
        commit_ts.flag = flags;
        const auto name = genNameForExchangeReceiver(1);
        Block early_block{{createNullableColumn<UInt64>({0, 11, 22}, {0, 0, 0}, name, MutSup::version_col_id)}};

        google::protobuf::RepeatedPtrField<tipb::Expr> filters;
        auto * condition = filters.Add();
        condition->set_tp(tipb::ExprType::ColumnRef);
        WriteBufferFromOwnString buffer;
        encodeDAGInt64(1, buffer); // Original scan: business column, commit_ts.
        condition->set_val(buffer.releaseStr());
        condition->mutable_field_type()->set_tp(TiDB::TypeLongLong);
        condition->mutable_field_type()->set_flag(flags);

        DAGExpressionAnalyzer analyzer{early_block, *ctx};
        ExpressionActionsChain chain;
        auto & step = analyzer.initAndGetLastStep(chain);
        auto [has_cast, casted_columns] = analyzer.buildExtraCastsAfterTS(step.actions, {1}, {commit_ts});
        ASSERT_TRUE(has_cast);
        step.actions->add(ExpressionAction::project(NamesWithAliases{{casted_columns[0], name}}));
        step.required_output.push_back(name);
        auto cast = chain.getLastActions();
        chain.finalize();
        Block filter_header = early_block.cloneEmpty();
        cast->execute(filter_header);
        ASSERT_EQ(filter_header.getByName(name).column_id, 0);
        EXPECT_THROW(remapColumnarFilterConditions(filters, {business, commit_ts}, filter_header), DB::Exception);

        // The production path maps offsets using the uncast projection, then
        // evaluates those offsets against the cast columns in the same order.
        const auto remapped = remapColumnarFilterConditions(filters, {business, commit_ts}, early_block);
        EXPECT_EQ(decodeDAGInt64(remapped[0].val()), 0);
        auto [before_where, filter_name, project] = analyzer.buildPushDownFilter(remapped, true);
        Block evaluation_block = early_block;
        cast->execute(evaluation_block);
        before_where->execute(evaluation_block);
        const auto & mask = evaluation_block.getByName(filter_name).column;
        ASSERT_EQ(mask->size(), 3);
        EXPECT_EQ((*mask)[0].safeGet<UInt64>(), 0);
        EXPECT_EQ((*mask)[1].safeGet<UInt64>(), 1);
        EXPECT_EQ((*mask)[2].safeGet<UInt64>(), 1);
        EXPECT_EQ(early_block.getByName(name).column_id, MutSup::version_col_id);
        EXPECT_EQ(early_block.getByName(name).type->getName(), "Nullable(UInt64)");
    }
}
CATCH

TEST_F(HiddenCommitTSColumnTest, ColumnarCommitTSMixedProjection)
try
{
    for (bool common_handle : {false, true})
    {
        tipb::Executor executor;
        executor.set_tp(tipb::TypeTableScan);
        auto * scan_pb = executor.mutable_tbl_scan();
        scan_pb->set_table_id(100);
        const std::vector<ColumnID>
            ids{10, MutSup::extra_commit_ts_col_id, MutSup::extra_table_id_col_id, MutSup::extra_handle_id, 11};
        for (auto id : ids)
        {
            auto * column = scan_pb->add_columns();
            column->set_column_id(id);
            column->set_tp(id == MutSup::extra_handle_id && common_handle ? TiDB::TypeVarString : TiDB::TypeLongLong);
            column->set_flag(TiDB::ColumnFlagNotNull | (id == 10 ? TiDB::ColumnFlagGeneratedColumn : 0));
        }
        DAGContext dag_context(1024);
        TiDBTableScan scan(&executor, "scan", dag_context);
        const auto table_info = genTableInfoForColumnarRead(scan);
        ASSERT_EQ(table_info.columns_size(), 2);
        EXPECT_EQ(table_info.columns(0).column_id(), MutSup::extra_handle_id);
        EXPECT_EQ(table_info.columns(0).tp(), common_handle ? TiDB::TypeVarString : TiDB::TypeLongLong);
        EXPECT_EQ(table_info.columns(1).column_id(), 11);
        auto [defines, table_id_index] = genColumnDefinesForDisaggregatedReadThroughColumnar(scan);
        ASSERT_EQ(defines->size(), 3);
        EXPECT_EQ(table_id_index, 2);
        EXPECT_EQ(defines->at(0).id, MutSup::version_col_id);
        EXPECT_EQ(defines->at(0).name, genNameForExchangeReceiver(1));
        EXPECT_EQ(defines->at(1).id, MutSup::extra_handle_id);
        EXPECT_EQ(defines->at(1).name, genNameForExchangeReceiver(3));
        EXPECT_EQ(defines->at(2).id, 11);
        EXPECT_EQ(defines->at(2).name, genNameForExchangeReceiver(4));
        const auto storage_scan = genTableScanForColumnarRead(scan);
        ASSERT_EQ(storage_scan.tbl_scan().columns_size(), ids.size());
        for (size_t i = 0; i < ids.size(); ++i)
            EXPECT_EQ(storage_scan.tbl_scan().columns(i).column_id(), getStorageColumnIDForColumnarRead(ids[i]));
    }
}
CATCH

TEST_F(HiddenCommitTSColumnTest, PushDownFilterAliasAndCast)
try
{
    // TiDB may request a hidden column with ColumnID=-5 (commit_ts). In TiFlash storage layer it is stored in
    // `_INTERNAL_VERSION` with ColumnID=VersionColumnID. When TiDB column type differs (e.g. Nullable(Int64)),
    // TiFlash should add a cast.

    TiDB::ColumnInfo commit_ts_ci;
    commit_ts_ci.id = MutSup::extra_commit_ts_col_id;
    commit_ts_ci.name = "commit_ts";
    commit_ts_ci.tp = TiDB::TypeLongLong; // Int64
    commit_ts_ci.flag = 0; // Nullable(Int64)

    TiDB::ColumnInfos table_scan_column_info{commit_ts_ci};

    // Use a single ColumnRef as filter condition: "where commit_ts".
    // This is enough to trigger:
    // 1) filter column id extraction (ColumnID=-5)
    // 2) aliasing from -5 to VersionColumnID in PushDownExecutor
    // 3) extra cast generation based on ColumnInfo (Nullable(Int64)) vs storage type (MutSup::getVersionColumnType())
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
    columns_to_read.emplace_back(MutSup::version_col_id, MutSup::version_column_name, MutSup::getVersionColumnType());

    auto executor = DM::PushDownExecutor::build(
        DM::EMPTY_RS_OPERATOR,
        nullptr, // ann_query_info
#if ENABLE_CLARA
        nullptr, // fts_query_info
#endif
        table_scan_column_info,
        pushed_down_filters,
        columns_to_read,
        nullptr, // column_range
        *ctx,
        log);

    ASSERT_TRUE(executor);
    ASSERT_TRUE(executor->filter_columns);
    ASSERT_EQ(executor->filter_columns->size(), 1);
    // Storage must read VersionColumnID, not -5.
    EXPECT_EQ(executor->filter_columns->at(0).id, MutSup::version_col_id);
    EXPECT_EQ(executor->filter_columns->at(0).name, MutSup::version_column_name);

    // Extra cast should exist because TiDB requires Nullable(Int64) while TiFlash storage uses MutSup::getVersionColumnType().
    ASSERT_TRUE(executor->extra_cast);

    Block block = Block{
        {toVec<UInt64>(MutSup::version_column_name, {1, 2, 3, 4})},
    };
    executor->extra_cast->execute(block);

    const auto expected_type = getDataTypeByColumnInfoForComputingLayer(commit_ts_ci);
    ASSERT_TRUE(block.has(MutSup::version_column_name));
    EXPECT_EQ(block.getByName(MutSup::version_column_name).type->getName(), expected_type->getName());
}
CATCH

TEST_F(HiddenCommitTSColumnTest, CastAfterTableScanForCommitTS)
try
{
    // Non-late-materialization path:
    // TiDB may request a hidden column with ColumnID=-5 (commit_ts). In TiFlash storage layer it is stored in
    // `_INTERNAL_VERSION` with type MutSup::getVersionColumnType() (currently UInt64). If TiDB column type differs
    // (e.g. Nullable(Int64)), TiFlash should add a cast after TableScan and keep the output column name unchanged.

    TiDB::ColumnInfo commit_ts_ci;
    commit_ts_ci.id = MutSup::extra_commit_ts_col_id;
    commit_ts_ci.name = "commit_ts";
    commit_ts_ci.tp = TiDB::TypeLongLong; // Int64
    commit_ts_ci.flag = 0; // Nullable

    TiDB::ColumnInfos table_scan_column_info{commit_ts_ci};
    std::vector<UInt8> may_need_add_cast_column{/*commit_ts*/ 1};

    Block block = Block{
        {toVec<UInt64>(MutSup::version_column_name, {1, 2, 3, 4})},
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
    project_cols.emplace_back(casted_columns[0], MutSup::version_column_name);
    actions->add(ExpressionAction::project(project_cols));
    step.required_output.push_back(MutSup::version_column_name);

    ExpressionActionsPtr extra_cast = chain.getLastActions();
    ASSERT_TRUE(extra_cast);
    chain.finalize();
    chain.clear();

    extra_cast->execute(block);

    const auto expected_type = getDataTypeByColumnInfoForComputingLayer(commit_ts_ci);
    ASSERT_TRUE(block.has(MutSup::version_column_name));
    EXPECT_EQ(block.getByName(MutSup::version_column_name).type->getName(), expected_type->getName());
}
CATCH

TEST_F(HiddenCommitTSColumnTest, RoughSetFilterAliasCommitTS)
try
{
    // Rough set filter (RSOperator) uses table_column_defines by ColumnID.
    // TiDB requests commit_ts as ColumnID=-5, but in TiFlash it is stored in `_INTERNAL_VERSION` (VersionColumnID).
    // Ensure rough set filter can correctly map ColumnID=-5 to VersionColumnID.

    TiDB::ColumnInfo commit_ts_ci;
    commit_ts_ci.id = MutSup::extra_commit_ts_col_id;
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
    tipb::FTSQueryInfo fts_query_info;
    google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters;
    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    std::vector<int> runtime_filter_ids;
    const int rf_max_wait_time_ms = 0;
    auto dag_query = std::make_unique<DAGQueryInfo>(
        filters,
        ann_query_info,
        fts_query_info,
        pushed_down_filters,
        used_indexes,
        scan_column_infos,
        runtime_filter_ids,
        rf_max_wait_time_ms,
        ctx->getTimezoneInfo());

    DM::ColumnDefines table_column_defines;
    table_column_defines.emplace_back(
        MutSup::version_col_id,
        MutSup::version_column_name,
        MutSup::getVersionColumnType());

    auto rs_operator
        = DM::RSOperator::build(dag_query, scan_column_infos, table_column_defines, /*enable_rs_filter*/ true, log);
    ASSERT_TRUE(rs_operator);

    const auto col_ids = rs_operator->getColumnIDs();
    ASSERT_EQ(col_ids.size(), 1);
    EXPECT_EQ(col_ids[0], MutSup::version_col_id);
}
CATCH

} // namespace DB::tests
