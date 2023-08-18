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

#pragma once

#include <Interpreters/ExpressionActions.h>

namespace DB
{
class Expand2;
using ExpressionActionsPtrVec = std::vector<ExpressionActionsPtr>;
using Expand2Ptr = std::shared_ptr<Expand2>;

class Expand2
{
public:
    /// Expand2 is used to level-project child block according projection expressions.
    /// In grouping sets usage, multi-distinct-agg cases, Rollup and Cute syntax scenarios,
    /// all of them resort to grouping-sets way to replicate the data to feed different groups,
    /// underlying which it's mainly supported by expand operator.
    ///
    /// eg: select count(a) from t group by a,b,c with rollup
    ///
    ///  expand: [a#1, a'#2, b#3, 0], [a#1, a'#2, null, 1], [a#1, null, null, 2]
    ///    +--projection: a#1, a'#2, b#3
    ///
    /// As shown above, every level of projection has individual projected expression to describe
    /// what this time they want, basically consisting of some columnRef and literal value.
    ///
    /// Since tiflash made the raw name for every expression it received in getActions(expr,cols),
    /// we should also cast all the leveled column to the same output alias name after evaluation.
    ///
    /// \param leveled_projections_actions
    /// \param level_alias_projections
    explicit Expand2(
        ExpressionActionsPtrVec projections_actions_,
        ExpressionActionsPtr before_expand_actions_,
        NamesWithAliasesVec projections_);
    Block next(const Block & block_cache, size_t i_th_projection);
    String getLevelProjectionDes() const;
    size_t getLevelProjectionNum() const;
    ExpressionActionsPtr & getBeforeExpandActions();

private:
    // for every leveled projection, make sure the colRef/literal has the correct fieldType.
    // eg: count(a) from t group by a,b,c rollup
    //
    // expand: [a#1, a'#2, b#3, 0], [a#1, a'#2, null, 1], [a#1, null, null, 2]
    //   |                                       ^    ^
    //   |                                       |    +-------- expand num literal should be constructed with grouping-id-col field type (Uint64) in planner side.
    //   +--projection: a#1, a'#2, b#3           +------------- ref-col should be changed as nullable and expand null literal should be constructed with based-col field type.
    //                        ^
    //                        +---------- below projection will cast a as a' for column copy usage.
    ExpressionActionsPtrVec leveled_projections_actions;
    // before_expand_actions serve as some nullable column preparation and prepend projection if needed.
    // (besides it will additional add generated column for header usage, while the expand logic itself doesn't use it)
    ExpressionActionsPtr before_expand_actions;
    // expand leveled projections should have a unified alias name to output
    NamesWithAliasesVec leveled_alias_projections;
};
} // namespace DB
