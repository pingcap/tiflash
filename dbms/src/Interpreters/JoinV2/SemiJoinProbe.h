// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/JoinV2/HashJoinProbe.h>
#include <Interpreters/JoinV2/SemiJoinProbeList.h>

namespace DB
{

#define SEMI_JOIN_PROBE_HELPER_TEMPLATE \
    template <                          \
        typename KeyGetter,             \
        ASTTableJoin::Kind kind,        \
        bool has_null_map,              \
        bool has_other_eq_cond_from_in, \
        bool tagged_pointer>

class HashJoin;
class SemiJoinProbeHelper : public JoinProbeHelperUtil
{
public:
    explicit SemiJoinProbeHelper(const HashJoin * join);

    static bool isSupported(ASTTableJoin::Kind kind, bool has_other_condition);

    Block probe(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

private:
    SEMI_JOIN_PROBE_HELPER_TEMPLATE
    Block probeImpl(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

    SEMI_JOIN_PROBE_HELPER_TEMPLATE
    void NO_INLINE probeFillColumns(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns);

    SEMI_JOIN_PROBE_HELPER_TEMPLATE
    void NO_INLINE
    probeFillColumnsPrefetch(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns);

    template <typename KeyGetter, ASTTableJoin::Kind kind>
    void handleOtherConditions(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

    template <
        typename KeyGetter,
        ASTTableJoin::Kind kind,
        bool has_other_eq_cond_from_in,
        bool has_other_cond,
        bool has_other_cond_null_map>
    void checkOtherConditionResults(
        JoinProbeContext & ctx,
        IColumn::Offsets & selective_offsets,
        const ColumnUInt8::Container * other_eq_column,
        ConstNullMapPtr other_eq_null_map,
        const ColumnUInt8::Container * other_column,
        ConstNullMapPtr other_null_map);

    Block genResultBlockForSemi(JoinProbeContext & ctx);
    Block genResultBlockForLeftOuterSemi(JoinProbeContext & ctx, bool has_other_eq_cond_from_in);

private:
    using FuncType = Block (SemiJoinProbeHelper::*)(JoinProbeContext &, JoinProbeWorkerData &);
    FuncType func_ptr_has_null = nullptr;
    FuncType func_ptr_no_null = nullptr;

    const HashJoin * join;
    const HashJoinPointerTable & pointer_table;
};

} // namespace DB
