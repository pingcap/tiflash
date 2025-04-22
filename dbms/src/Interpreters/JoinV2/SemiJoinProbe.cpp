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

#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/JoinV2/SemiJoinProbe.h>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

using enum ASTTableJoin::Kind;

enum class SemiJoinProbeResType : UInt8
{
    FALSE_VALUE,
    TRUE_VALUE,
    NULL_VALUE,
};

SemiJoinProbeHelper::SemiJoinProbeHelper(const HashJoin * join)
    : JoinProbeHelperUtil(join->settings, join->row_layout)
    , join(join)
    , pointer_table(join->pointer_table)
{
    RUNTIME_CHECK(join->has_other_condition);

#define CALL2(KeyGetter, JoinType, tagged_pointer)                                                      \
    {                                                                                                   \
        func_ptr_has_null = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, true, tagged_pointer>; \
        func_ptr_no_null = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, false, tagged_pointer>; \
    }

#define CALL1(KeyGetter, JoinType)               \
    {                                            \
        if (pointer_table.enableTaggedPointer()) \
            CALL2(KeyGetter, JoinType, true)     \
        else                                     \
            CALL2(KeyGetter, JoinType, false)    \
    }

#define CALL(KeyGetter)                                                                                    \
    {                                                                                                      \
        auto kind = join->kind;                                                                            \
        if (kind == Semi)                                                                                  \
            CALL1(KeyGetter, Semi)                                                                         \
        else if (kind == Anti)                                                                             \
            CALL1(KeyGetter, Anti)                                                                         \
        else if (kind == LeftOuterSemi)                                                                    \
            CALL1(KeyGetter, LeftOuterSemi)                                                                \
        else if (kind == LeftOuterAnti)                                                                    \
            CALL1(KeyGetter, LeftOuterAnti)                                                                \
        else                                                                                               \
            throw Exception(                                                                               \
                fmt::format("Logical error: unknown combination of JOIN {}", magic_enum::enum_name(kind)), \
                ErrorCodes::LOGICAL_ERROR);                                                                \
    }

    switch (join->method)
    {
#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        CALL(KeyGetterType##METHOD);                                                       \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception(
            fmt::format("Unknown JOIN keys variant {}.", magic_enum::enum_name(join->method)),
            ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
#undef CALL
#undef CALL1
#undef CALL2
}

Block SemiJoinProbeHelper::probe(JoinProbeContext & context, JoinProbeWorkerData & wd)
{
    if (context.null_map)
        return (this->*func_ptr_has_null)(context, wd);
    else
        return (this->*func_ptr_no_null)(context, wd);
}

template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer>
Block SemiJoinProbeHelper::probeImpl(JoinProbeContext & context, JoinProbeWorkerData & wd)
{
    if unlikely (context.rows == 0)
        return join->output_block_after_finalize;

    auto * probe_list = static_cast<SemiJoinPendingProbeList *>(context.semi_join_pending_probe_list.get());
    RUNTIME_CHECK(probe_list->size() == context.rows);
}


} // namespace DB
