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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Common/HashTable/Hash.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <fmt/core.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

ColumnConst::ColumnConst(const ColumnPtr & data_, size_t s)
    : data(data_)
    , s(s)
{
    /// Squash Const of Const.
    while (const auto * const_data = typeid_cast<const ColumnConst *>(data.get()))
        data = const_data->getDataColumnPtr();

    if (data->size() != 1)
        throw Exception(
            fmt::format("Incorrect size of nested column in constructor of ColumnConst: {}, must be 1.", data->size()),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
}

ColumnPtr ColumnConst::convertToFullColumn() const
{
    return data->replicate(Offsets(1, s));
}

ColumnPtr ColumnConst::filter(const Filter & filt, ssize_t /*result_size_hint*/) const
{
    if (s != filt.size())
        throw Exception(
            fmt::format("Size of filter ({}) doesn't match size of column ({})", filt.size(), s),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return ColumnConst::create(data, countBytesInFilter(filt));
}

ColumnPtr ColumnConst::replicateRange(size_t /*start_row*/, size_t end_row, const IColumn::Offsets & offsets) const
{
    if (s != offsets.size())
        throw Exception(
            fmt::format("Size of offsets ({}) doesn't match size of column ({})", offsets.size(), s),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(end_row <= s);
    size_t replicated_size = 0 == s ? 0 : (offsets[end_row - 1]);
    return ColumnConst::create(data, replicated_size);
}


ColumnPtr ColumnConst::permute(const Permutation & perm, size_t limit) const
{
    if (limit == 0)
        limit = s;
    else
        limit = std::min(s, limit);

    if (perm.size() < limit)
        throw Exception(
            fmt::format("Size of permutation ({}) is less than required ({})", perm.size(), limit),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return ColumnConst::create(data, limit);
}

MutableColumns ColumnConst::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    RUNTIME_CHECK_MSG(
        s == selector.size(),
        "Size of selector ({}) doesn't match size of column ({})",
        selector.size(),
        s);

    return scatterImplForColumnConst(num_columns, selector);
}

MutableColumns ColumnConst::scatter(
    ColumnIndex num_columns,
    const Selector & selector,
    const BlockSelectivePtr & selective) const
{
    const auto selective_rows = selective->size();
    RUNTIME_CHECK_MSG(
        selective_rows == selector.size(),
        "Size of selector ({}) doesn't match size of selective column ({})",
        selector.size(),
        selective_rows);

    return scatterImplForColumnConst(num_columns, selector);
}

MutableColumns ColumnConst::scatterImplForColumnConst(ColumnIndex num_columns, const Selector & selector) const
{
    std::vector<size_t> counts = countColumnsSizeInSelector(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        res[i] = cloneResized(counts[i]);

    return res;
}

void ColumnConst::scatterTo(ScatterColumns & columns, const Selector & selector) const
{
    RUNTIME_CHECK_MSG(
        s == selector.size(),
        "Size of selector ({}) doesn't match size of column ({})",
        selector.size(),
        s);
    scatterToImplForColumnConst(columns, selector);
}

void ColumnConst::scatterTo(ScatterColumns & columns, const Selector & selector, const BlockSelectivePtr & selective)
    const
{
    const auto selective_rows = selective->size();
    RUNTIME_CHECK_MSG(
        selective_rows == selector.size(),
        "Size of selector ({}) doesn't match size of column ({})",
        selector.size(),
        selective_rows);
    scatterToImplForColumnConst(columns, selector);
}

void ColumnConst::scatterToImplForColumnConst(ScatterColumns & columns, const Selector & selector) const
{
    ColumnIndex num_columns = columns.size();
    std::vector<size_t> counts = countColumnsSizeInSelector(num_columns, selector);

    for (size_t i = 0; i < num_columns; ++i)
        columns[i]->insertRangeFrom(*this, 0, counts[i]);
}

void ColumnConst::getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res)
    const
{
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

void ColumnConst::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    RUNTIME_CHECK_MSG(
        hash.getData().size() != s,
        "Size of WeakHash32({}) does not match size of column({})",
        hash.getData().size(),
        s);
    updateWeakHash32(hash, collator, sort_key_container);
}

void ColumnConst::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container,
    const BlockSelectivePtr & selective_ptr) const
{
    const auto selective_rows = selective_ptr->size();
    RUNTIME_CHECK_MSG(
        hash.getData().size() != selective_rows,
        "Size of WeakHash32({}) does not match size of selective column({})",
        hash.getData().size(),
        selective_rows);

    updateWeakHash32Impl(hash, collator, sort_key_container);
}

void ColumnConst::updateWeakHash32Impl(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    WeakHash32 element_hash(1);
    data->updateWeakHash32(element_hash, collator, sort_key_container);
    size_t data_hash = element_hash.getData()[0];

    for (auto & value : hash.getData())
        value = intHashCRC32(data_hash, value);
}
} // namespace DB
