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
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinV2/HashJoinBuild.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/JoinV2/HashJoinPointerTable.h>
#include <Interpreters/JoinV2/HashJoinProbe.h>
#include <Interpreters/JoinV2/HashJoinRowLayout.h>
#include <Interpreters/JoinV2/HashJoinSettings.h>


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

    void insertFromBlock(const Block & block, size_t stream_index);

    /// Return true if it is the last build worker.
    bool finishOneBuild(size_t stream_index);
    /// Return true if it is the last probe worker.
    bool finishOneProbe(size_t stream_index);

    bool buildPointerTable(size_t stream_index);

    Block joinBlock(JoinProbeContext & context, size_t stream_index);

    void removeUselessColumn(Block & block) const;
    Block removeUselessColumnForOutput(const Block & block) const;

    const Block & getOutputBlock() const { return finalized ? output_block_after_finalize : output_block; }
    const Names & getRequiredColumns() const { return required_columns; }
    void finalize(const Names & parent_require);
    bool isFinalize() const { return finalized; }

    size_t getBuildConcurrency() const { return build_concurrency; }
    size_t getProbeConcurrency() const { return probe_concurrency; }

    const JoinProfileInfoPtr & getProfileInfo() const { return profile_info; }

    Block getProbeBufferedResultBlock(size_t stream_index)
    {
        auto & wd = probe_workers_data[stream_index];
        if (has_other_condition)
            return std::move(wd.result_block_for_other_condition);
        if (wd.result_block)
        {
            auto res_block = removeUselessColumnForOutput(wd.result_block);
            wd.result_block = {};
            return res_block;
        }
        return {};
    }

private:
    void initRowLayoutAndHashJoinMethod();

    void workAfterBuildFinish();

    Block handleOtherConditions(size_t stream_index);

private:
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

    NamesAndTypes output_columns;
    Block output_block;
    NamesAndTypes output_columns_after_finalize;
    Block output_block_after_finalize;
    NameSet output_column_names_set_after_finalize;
    NameSet output_columns_names_set_for_other_condition_after_finalize;
    Names required_columns;
    bool finalized = false;

    /// Row containers
    std::vector<std::unique_ptr<MultipleRowContainer>> multi_row_containers;

    /// Build phase
    size_t build_concurrency = 0;
    std::vector<JoinBuildWorkerData> build_workers_data;
    std::atomic<size_t> active_build_worker = 0;

    /// Probe phase
    size_t probe_concurrency = 0;
    std::vector<JoinProbeWorkerData> probe_workers_data;
    std::atomic<size_t> active_probe_worker = 0;

    HashJoinPointerTable pointer_table;

    const JoinProfileInfoPtr profile_info = std::make_shared<JoinProfileInfo>();

    /// For other condition
    const IColumn::Offsets base_offsets;
};

using HashJoinPtr = std::shared_ptr<HashJoin>;

} // namespace DB
