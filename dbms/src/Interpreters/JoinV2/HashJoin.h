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

#pragma once

#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Pipeline/Schedule/Tasks/OneTimeNotifyFuture.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/JoinV2/HashJoinBuild.h>
#include <Interpreters/JoinV2/HashJoinBuildScannerAfterProbe.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/JoinV2/HashJoinPointerTable.h>
#include <Interpreters/JoinV2/HashJoinProbe.h>
#include <Interpreters/JoinV2/HashJoinRowLayout.h>
#include <Interpreters/JoinV2/HashJoinSettings.h>
#include <Interpreters/JoinV2/SemiJoinProbe.h>


namespace DB
{

class HashJoin
{
public:
    HashJoin(
        const Names & key_names_left_,
        const Names & key_names_right_,
        ASTTableJoin::Kind kind_,
        const String & req_id,
        const NamesAndTypes & output_columns_,
        const TiDB::TiDBCollators & collators_,
        const JoinNonEqualConditions & non_equal_conditions_,
        const Settings & settings,
        const String & match_helper_name_);

    void initBuild(const Block & sample_block, size_t build_concurrency_ = 1);

    void initProbe(const Block & sample_block, size_t probe_concurrency_ = 1);

    /// Return true if it is the last build row worker.
    bool finishOneBuildRow(size_t stream_index);
    /// Return true if it is the last probe worker.
    bool finishOneProbe(size_t stream_index);
    /// Return true if all probe work has finished.
    bool isAllProbeFinished();

    void buildRowFromBlock(const Block & block, size_t stream_index);
    bool buildPointerTable(size_t stream_index);

    Block probeBlock(JoinProbeContext & ctx, size_t stream_index);
    Block probeLastResultBlock(size_t stream_index);

    bool needProbeScanBuildSide() const;
    Block scanBuildSideAfterProbe(size_t stream_index);

    void removeUselessColumn(Block & block) const;
    /// Block's schema must be all_sample_block_pruned.
    Block removeUselessColumnForOutput(const Block & block) const;

    void initOutputBlock(Block & block) const;
    const Block & getOutputBlock() const { return finalized ? output_block_after_finalize : output_block; }
    const Names & getRequiredColumns() const { return required_columns; }
    void finalize(const Names & parent_require);
    bool isFinalize() const { return finalized; }

    size_t getBuildConcurrency() const { return build_concurrency; }
    size_t getProbeConcurrency() const { return probe_concurrency; }

    const JoinProfileInfoPtr & getProfileInfo() const { return profile_info; }

    const OneTimeNotifyFuturePtr & getWaitProbeFinishFuture() { return wait_probe_finished_future; }

private:
    void initRowLayoutAndHashJoinMethod();

    void workAfterBuildRowFinish();

    bool shouldCheckLateMaterialization() const
    {
        bool is_any_semi_join = isSemiFamily(kind) || isLeftOuterSemiFamily(kind) || isRightSemiFamily(kind);
        return has_other_condition && !is_any_semi_join
            && row_layout.other_column_count_for_other_condition < row_layout.other_column_indexes.size();
    }

private:
    friend JoinBuildHelper;
    friend JoinProbeHelper;
    friend SemiJoinProbeHelper;
    friend JoinBuildScannerAfterProbe;

    static const DataTypePtr match_helper_type;

    const ASTTableJoin::Kind kind;
    const String join_req_id;

    /// Names of key columns (columns for equi-JOIN) in "left" table (in the order they appear in USING clause).
    Names key_names_left;
    /// Names of key columns (columns for equi-JOIN) in "right" table (in the order they appear in USING clause).
    Names key_names_right;

    /// collators for the join key
    const TiDB::TiDBCollators collators;

    const JoinNonEqualConditions non_equal_conditions;

    const HashJoinSettings settings;

    // only use for left outer semi joins.
    const String match_helper_name;

    const LoggerPtr log;

    const bool has_other_condition;

    bool build_initialized = false;
    bool probe_initialized = false;

    HashJoinRowLayout row_layout;
    HashJoinKeyMethod method = HashJoinKeyMethod::Empty;

    /// Block with columns from the right-side table.
    Block right_sample_block;
    Block right_sample_block_pruned;
    /// Block with columns from the left-side table.
    Block left_sample_block;
    Block left_sample_block_pruned;
    /// Block with columns from left-side and right-side table.
    Block all_sample_block_pruned;

    /// Maps each column in all_sample_block_pruned to its index in output_block_after_finalize.
    // <= 0 means the column is not in the output_block_after_finalize.
    std::vector<ssize_t> output_column_indexes;

    NamesAndTypes output_columns;
    Block output_block;
    NamesAndTypes output_columns_after_finalize;
    Block output_block_after_finalize;
    NameSet output_column_names_set_after_finalize;
    NameSet output_columns_names_set_for_other_condition_after_finalize;
    Names required_columns;
    NameSet required_columns_names_set_for_other_condition;
    bool finalized = false;

    /// Row containers
    std::vector<std::unique_ptr<MultipleRowContainer>> multi_row_containers;
    /// Non-joined blocks
    NonJoinedBlocks non_joined_blocks;

    /// Build row phase
    size_t build_concurrency = 0;
    std::vector<JoinBuildWorkerData> build_workers_data;
    std::atomic<size_t> active_build_worker = 0;

    HashJoinPointerTable pointer_table;

    /// Probe phase
    size_t probe_concurrency = 0;
    std::vector<JoinProbeWorkerData> probe_workers_data;
    std::atomic<size_t> active_probe_worker = 0;
    OneTimeNotifyFuturePtr wait_probe_finished_future;
    std::unique_ptr<JoinProbeHelper> join_probe_helper;
    std::unique_ptr<SemiJoinProbeHelper> semi_join_probe_helper;
    /// Probe scan build side
    std::unique_ptr<JoinBuildScannerAfterProbe> join_build_scanner_after_probe;

    const JoinProfileInfoPtr profile_info = std::make_shared<JoinProfileInfo>();

    /// For other condition
    BoolVec left_required_flag_for_other_condition;
};

using HashJoinPtr = std::shared_ptr<HashJoin>;

} // namespace DB
