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
#include <Interpreters/ExpressionActions.h>
#include <Core/Block.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

enum class HashJoinKeyMethod
{
    OneFixed,
    AllFixed,
    Serialized,
};

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
    ASTTableJoin::Kind kind;
	bool has_other_condition;
    String join_req_id;

	Sizes key_sizes;

	/// Names of key columns (columns for equi-JOIN) in "left" table (in the order they appear in USING clause).
    const Names key_names_left;
    /// Names of key columns (columns for equi-JOIN) in "right" table (in the order they appear in USING clause).
    const Names key_names_right;

    /// Block with columns from the right-side table.
    Block right_sample_block;
    /// Block with key columns in the same order they appear in the right-side table.
    Block right_sample_block_only_keys;

    NamesAndTypes output_columns;
    Block output_block;
    NamesAndTypes output_columns_after_finalize;
    Block output_block_after_finalize;
    NameSet output_column_names_set_after_finalize;
    NameSet output_columns_names_set_for_other_condition_after_finalize;
    Names required_columns;
    bool finalized = false;

	const LoggerPtr log;
};


} // namespace DB