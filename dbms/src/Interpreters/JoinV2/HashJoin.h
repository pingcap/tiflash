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

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinV2/HashJoinBuild.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/JoinV2/HashJoinRowSchema.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

class HashJoin
{
    HashJoin(
        const Names & key_names_left_,
        const Names & key_names_right_,
        ASTTableJoin::Kind kind_,
        const String & req_id,
        const NamesAndTypes & output_columns_,
        const TiDB::TiDBCollators & collators_,
        const JoinNonEqualConditions & non_equal_conditions_,
        size_t max_block_size);

    void initBuild(const Block & sample_block, size_t build_concurrency_ = 1);

    void initProbe(const Block & sample_block, size_t probe_concurrency_ = 1);

    void insertFromBlock(const Block & block, size_t stream_index);

    void checkTypes(const Block & block) const;

    void finalize(const Names & parent_require);

private:
    void initRowSchemaAndHashJoinMethod();

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

    const size_t max_block_size;

    const LoggerPtr log;

    bool has_other_condition;

    bool initialized = false;

    HashJoinRowSchema row_schema;
    HashJoinKeyMethod method = HashJoinKeyMethod::Empty;

    /// Block with columns from the right-side table after finalized.
    Block right_sample_block;

    NamesAndTypes output_columns;
    Block output_block;
    NamesAndTypes output_columns_after_finalize;
    Block output_block_after_finalize;
    NameSet output_column_names_set_after_finalize;
    NameSet output_columns_names_set_for_other_condition_after_finalize;
    Names required_columns;
    bool finalized = false;

    std::vector<std::unique_ptr<ColumnRowsWithLock>> partition_column_rows_with_lock;

    /// Build phase
    size_t build_concurrency;
    std::vector<std::unique_ptr<BuildWorkerData>> build_workers_data;
};

} // namespace DB