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
        const Settings & settings);

    void initBuild(const Block & sample_block, size_t build_concurrency_ = 1);

    void initProbe(const Block & sample_block, size_t probe_concurrency_ = 1);

    void insertFromBlock(const Block & block, size_t stream_index);

    /// Return true if it is the last build thread.
    bool finishOneBuild(size_t stream_index);

    bool buildPointerTable(size_t stream_index);

    Block joinBlock(JoinProbeContext & context, size_t stream_index);

    Block removeUselessColumn(Block & block) const;

    void finalize(const Names & parent_require);

private:
    void initRowLayoutAndHashJoinMethod();

    Block doJoinBlock(JoinProbeContext & context, size_t stream_index);

    void workAfterBuildFinish();

private:
    const ASTTableJoin::Kind kind;
    const String join_req_id;
    const bool may_probe_side_expanded_after_join;

    /// Names of key columns (columns for equi-JOIN) in "left" table (in the order they appear in USING clause).
    Names key_names_left;
    /// Names of key columns (columns for equi-JOIN) in "right" table (in the order they appear in USING clause).
    Names key_names_right;

    /// collators for the join key
    const TiDB::TiDBCollators collators;

    const JoinNonEqualConditions non_equal_conditions;

    HashJoinSettings settings;

    const LoggerPtr log;

    bool has_other_condition;

    bool initialized = false;

    HashJoinRowLayout row_layout;
    HashJoinKeyMethod method = HashJoinKeyMethod::Empty;

    /// Block with columns from the right-side table after finalized.
    Block right_sample_block;
    /// Block with columns from the left-side table after finalized.
    Block left_sample_block;

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
    size_t build_concurrency;
    std::vector<JoinBuildWorkerData> build_workers_data;
    std::atomic<size_t> active_build_threads = 0;

    /// Probe phase
    size_t probe_concurrency;
    std::vector<JoinProbeWorkerData> probe_workers_data;

    HashJoinPointerTable pointer_table;
};

} // namespace DB
