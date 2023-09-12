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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/sortBlock.h>
#include <TiDB/Collation/Collator.h>
#include <TiDB/Collation/CollatorUtils.h>
#include <common/defines.h>

#if defined(APPLY_FOR_TYPE) || defined(M) || defined(CONCAT)
static_assert(false);
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_COLLATION;
}


using ColumnsWithSortDescriptions = std::vector<std::pair<const IColumn *, SortColumnDescription>>;

static ColumnsWithSortDescriptions getColumnsWithSortDescription(
    const Block & block,
    const SortDescription & description)
{
    size_t size = description.size();
    ColumnsWithSortDescriptions res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const IColumn * column = !description[i].column_name.empty()
            ? block.getByName(description[i].column_name).column.get()
            : block.safeGetByPosition(description[i].column_number).column.get();

        res.emplace_back(column, description[i]);
    }

    return res;
}

ALWAYS_INLINE static inline bool NeedCollation(const IColumn * column, const SortColumnDescription & description)
{
    if (!description.collator)
        return false;
    const auto * not_null_column
        = column->isColumnNullable() ? typeid_cast<const ColumnNullable *>(column)->getNestedColumnPtr().get() : column;

    if (not_null_column->isColumnConst())
        return false;

    if (!typeid_cast<const ColumnString *>(not_null_column))
        throw Exception("Collations could be specified only for String columns.", ErrorCodes::BAD_COLLATION);

    return true;
}

#define APPLY_FOR_TYPE(M) \
    M(UInt64)             \
    M(Int64)              \
    M(StringBin)          \
    M(StringBinPadding)   \
    M(StringWithCollatorGeneric)

#define CONCAT(x, y) x##y

enum class FastPathType
{
#define M(NAME) NAME,
    APPLY_FOR_TYPE(M)
#undef M
};

constexpr size_t max_fast_path_num = 2;

template <typename T>
struct ColumnVecCompare
{
    static inline const ColumnVector<T> * intoTarget(const IColumn * column)
    {
        return static_cast<const ColumnVector<T> *>(column);
    }

    template <size_t index>
    ALWAYS_INLINE static inline int compareAt(
        size_t a,
        size_t b,
        const ColumnsWithSortDescriptions & columns_with_sort_desc)
    {
        const auto & desc = columns_with_sort_desc[index];
        const auto * column = intoTarget(desc.first);
        return column->compareAt(a, b, *column, 0) * desc.second.direction;
    }
};

template <FastPathType type>
struct ColumnStringCompare
{
    static inline const ColumnString * intoTarget(const IColumn * column)
    {
        return static_cast<const ColumnString *>(column);
    }

    template <size_t index>
    ALWAYS_INLINE static inline int compareAt(
        size_t a,
        size_t b,
        const ColumnsWithSortDescriptions & columns_with_sort_desc)
    {
        const auto & desc = columns_with_sort_desc[index];
        const auto * column = intoTarget(desc.first);
        int ret = 0;
        {
            auto str_a = column->getDataAt(a);
            auto str_b = column->getDataAt(b);

            if constexpr (type == FastPathType::StringBinPadding)
            {
                ret = BinCollatorCompare<true>(str_a.data, str_a.size, str_b.data, str_b.size);
            }
            else if constexpr (type == FastPathType::StringBin)
            {
                ret = BinCollatorCompare<false>(str_a.data, str_a.size, str_b.data, str_b.size);
            }
            else
            {
                ret = desc.second.collator->compare(str_a.data, str_a.size, str_b.data, str_b.size);
            }
        }
        return desc.second.direction * ret;
    }
};

using ColumnCompareUInt64 = ColumnVecCompare<UInt64>;
using ColumnCompareInt64 = ColumnVecCompare<Int64>;
using ColumnCompareStringBinPadding = ColumnStringCompare<FastPathType::StringBinPadding>;
using ColumnCompareStringBin = ColumnStringCompare<FastPathType::StringBin>;
using ColumnCompareStringWithCollatorGeneric = ColumnStringCompare<FastPathType::StringWithCollatorGeneric>;

// only for uint64, int64, string
template <typename ColumnCmpA, typename ColumnCmpB>
struct MultiColumnSortFastPath
{
    const ColumnsWithSortDescriptions & columns_with_sort_desc;

    explicit MultiColumnSortFastPath(const ColumnsWithSortDescriptions & columns_with_sort_desc_)
        : columns_with_sort_desc(columns_with_sort_desc_)
    {}

    ALWAYS_INLINE inline int compareAt(size_t a, size_t b) const
    {
        constexpr size_t index_a = 0, index_b = 1;

        int ret = 0;
        {
            ret = ColumnCmpA::template compareAt<index_a>(a, b, columns_with_sort_desc);
        }
        if (!ret)
        {
            ret = ColumnCmpB::template compareAt<index_b>(a, b, columns_with_sort_desc);
        }
        return ret;
    }

    ALWAYS_INLINE inline bool operator()(size_t a, size_t b) const
    {
        int ret = compareAt(a, b);
        return ret < 0;
    }
};

struct FastSortDesc : boost::noncopyable
{
    const ColumnsWithSortDescriptions & columns_with_sort_desc;
    std::vector<bool> need_collations;

    bool has_collation{false};

    bool can_use_fast_path = false;
    std::array<FastPathType, max_fast_path_num> type_for_fast_path{};
    size_t fast_path_cnt = 0;

    ALWAYS_INLINE static bool isUInt64(const IColumn * column) { return typeid_cast<const ColumnUInt64 *>(column); }
    ALWAYS_INLINE static bool isInt64(const IColumn * column) { return typeid_cast<const ColumnInt64 *>(column); }
    ALWAYS_INLINE static bool isString(const IColumn * column) { return typeid_cast<const ColumnString *>(column); }

    inline void addFastPathType(FastPathType tp) { type_for_fast_path[fast_path_cnt++] = tp; }

    explicit FastSortDesc(const ColumnsWithSortDescriptions & columns_with_sort_desc_)
        : columns_with_sort_desc(columns_with_sort_desc_)
    {
        need_collations.reserve(columns_with_sort_desc.size());

        can_use_fast_path = columns_with_sort_desc.size() == max_fast_path_num;

        for (const auto & sort_desc : columns_with_sort_desc)
        {
            auto need_collation = DB::NeedCollation(sort_desc.first, sort_desc.second);
            if (need_collation)
            {
                has_collation = true;
                if (isString(sort_desc.first))
                {
                    // only when column is string(not nullable)
                    auto collator_type = TiDB::GetTiDBCollatorType(sort_desc.second.collator);
                    if (can_use_fast_path)
                    {
                        switch (collator_type)
                        {
                        case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
                        case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
                        case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
                        case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
                        {
                            addFastPathType(FastPathType::StringBinPadding);
                            break;
                        }
                        case TiDB::ITiDBCollator::CollatorType::BINARY:
                        {
                            addFastPathType(FastPathType::StringBin);
                            break;
                        }
                        default:
                        {
                            addFastPathType(FastPathType::StringWithCollatorGeneric);
                            break;
                        }
                        }
                    }
                }
                else
                {
                    // nullable
                    can_use_fast_path = false;
                }
            }
            else if (can_use_fast_path)
            {
                if (isUInt64(sort_desc.first))
                {
                    addFastPathType(FastPathType::UInt64);
                }
                else if (isInt64(sort_desc.first))
                {
                    addFastPathType(FastPathType::Int64);
                }
                else
                {
                    can_use_fast_path = false;
                }
            }
            need_collations.emplace_back(need_collation);
        }
    }

    ALWAYS_INLINE inline size_t size() const { return columns_with_sort_desc.size(); }

    ALWAYS_INLINE inline int compareAt(size_t col_index, size_t a, size_t b) const
    {
        const auto & column = columns_with_sort_desc[col_index];

        if (!need_collations[col_index])
        {
            return column.second.direction
                * column.first->compareAt(a, b, *column.first, column.second.nulls_direction);
        }
        else
        {
            return column.second.direction
                * column.first->compareAt(a, b, *column.first, column.second.nulls_direction, *column.second.collator);
        }
    }
};

struct PartialSortingLess
{
    const ColumnsWithSortDescriptions & columns;

    explicit PartialSortingLess(const ColumnsWithSortDescriptions & columns_)
        : columns(columns_)
    {}

    ALWAYS_INLINE inline bool operator()(size_t a, size_t b) const
    {
        int res = 0;
        for (const auto & column : columns)
        {
            res = column.second.direction * column.first->compareAt(a, b, *column.first, column.second.nulls_direction);
            if (res)
                break;
        }
        return res < 0;
    }
};

struct PartialSortingLessWithCollation
{
    const FastSortDesc & fast_sort_desc;

    explicit PartialSortingLessWithCollation(const FastSortDesc & desc_)
        : fast_sort_desc(desc_)
    {
        assert(fast_sort_desc.has_collation);
    }

    ALWAYS_INLINE inline bool operator()(size_t a, size_t b) const
    {
        int res = 0;
        for (size_t i = 0; i < fast_sort_desc.size(); ++i)
        {
            res = fast_sort_desc.compareAt(i, a, b);
            if (res)
                break;
        }
        return res < 0;
    }
};

template <typename F>
ALWAYS_INLINE static inline void PermutationSort(IColumn::Permutation & perm, size_t limit, F && fn_cmp)
{
    if (limit)
        std::partial_sort(perm.begin(), perm.begin() + limit, perm.end(), fn_cmp);
    else
        std::sort(perm.begin(), perm.end(), fn_cmp);
}

template <size_t N>
struct FastPathPermutationSort
{
};

template <>
struct FastPathPermutationSort<2>
{
    template <typename A, typename B>
    ALWAYS_INLINE static inline void FastPathPermutationSort_P2(
        const FastSortDesc & desc,
        IColumn::Permutation & perm,
        size_t limit)
    {
        MultiColumnSortFastPath<A, B> cmp{desc.columns_with_sort_desc};
        return PermutationSort(perm, limit, cmp);
    }

    template <typename A>
    ALWAYS_INLINE static inline void FastPathPermutationSort_P1(
        const FastSortDesc & desc,
        IColumn::Permutation & perm,
        size_t limit)
    {
        constexpr size_t index = 1;

#define M(NAME)                                                                               \
    case FastPathType::NAME:                                                                  \
    {                                                                                         \
        return FastPathPermutationSort_P2<A, CONCAT(ColumnCompare, NAME)>(desc, perm, limit); \
    }

        switch (desc.type_for_fast_path[index])
        {
            APPLY_FOR_TYPE(M)
        }
#undef M
    }

    void operator()(const FastSortDesc & desc, IColumn::Permutation & perm, size_t limit) const
    {
        constexpr size_t index = 0;

#define M(NAME)                                                                            \
    case FastPathType::NAME:                                                               \
    {                                                                                      \
        return FastPathPermutationSort_P1<CONCAT(ColumnCompare, NAME)>(desc, perm, limit); \
    }

        switch (desc.type_for_fast_path[index])
        {
            APPLY_FOR_TYPE(M)
        }
#undef M
    }
};

void sortBlock(Block & block, const SortDescription & description, size_t limit)
{
    if (!block)
        return;

    /// If only one column to sort by
    if (description.size() == 1)
    {
        bool reverse = description[0].direction < 0;

        const IColumn * column = !description[0].column_name.empty()
            ? block.getByName(description[0].column_name).column.get()
            : block.safeGetByPosition(description[0].column_number).column.get();

        IColumn::Permutation perm;
        if (NeedCollation(column, description[0]))
            column->getPermutation(*description[0].collator, reverse, limit, description[0].nulls_direction, perm);
        else
            column->getPermutation(reverse, limit, description[0].nulls_direction, perm);

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->permute(perm, limit);
    }
    else
    {
        size_t size = block.rows();
        IColumn::Permutation perm(size);
        for (size_t i = 0; i < size; ++i)
            perm[i] = i;

        if (limit >= size)
            limit = 0;

        ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);
        const auto collator_desc = FastSortDesc{columns_with_sort_desc};
        if (collator_desc.can_use_fast_path)
        {
            assert(collator_desc.fast_path_cnt == max_fast_path_num);

            FastPathPermutationSort<max_fast_path_num>{}(collator_desc, perm, limit);

            // TODO: optimize for other cases
        }
        else if (collator_desc.has_collation)
        {
            PartialSortingLessWithCollation less_with_collation(collator_desc);
            PermutationSort(perm, limit, less_with_collation);
        }
        else
        {
            PartialSortingLess less(columns_with_sort_desc);
            PermutationSort(perm, limit, less);
        }

        for (size_t i = 0; i < block.columns(); ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->permute(perm, limit);
    }
}


void stableGetPermutation(
    const Block & block,
    const SortDescription & description,
    IColumn::Permutation & out_permutation)
{
    if (!block)
        return;

    size_t size = block.rows();
    out_permutation.resize(size);
    for (size_t i = 0; i < size; ++i)
        out_permutation[i] = i;

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

    std::stable_sort(out_permutation.begin(), out_permutation.end(), PartialSortingLess(columns_with_sort_desc));
}


bool isAlreadySorted(const Block & block, const SortDescription & description)
{
    if (!block)
        return true;

    size_t rows = block.rows();

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

    PartialSortingLess less(columns_with_sort_desc);

    /** If the rows are not too few, then let's make a quick attempt to verify that the block is not sorted.
     * Constants - at random.
     */
    static constexpr size_t num_rows_to_try = 10;
    if (rows > num_rows_to_try * 5)
    {
        for (size_t i = 1; i < num_rows_to_try; ++i)
        {
            size_t prev_position = rows * (i - 1) / num_rows_to_try;
            size_t curr_position = rows * i / num_rows_to_try;

            if (less(curr_position, prev_position))
                return false;
        }
    }

    for (size_t i = 1; i < rows; ++i)
        if (less(i, i - 1))
            return false;

    return true;
}


void stableSortBlock(Block & block, const SortDescription & description)
{
    if (!block)
        return;

    IColumn::Permutation perm;
    stableGetPermutation(block, description, perm);

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
        block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->permute(perm, 0);
}

} // namespace DB
