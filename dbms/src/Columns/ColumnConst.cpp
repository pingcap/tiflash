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
    while (const ColumnConst * const_data = typeid_cast<const ColumnConst *>(data.get()))
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

ColumnPtr ColumnConst::replicate(const Offsets & offsets) const
{
    if (s != offsets.size())
        throw Exception(
            fmt::format("Size of offsets ({}) doesn't match size of column ({})", offsets.size(), s),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    size_t replicated_size = 0 == s ? 0 : offsets.back();
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
    if (s != selector.size())
        throw Exception(
            fmt::format("Size of selector ({}) doesn't match size of column ({})", selector.size(), s),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<size_t> counts = countColumnsSizeInSelector(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        res[i] = cloneResized(counts[i]);

    return res;
}

void ColumnConst::getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res) const
{
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

void ColumnConst::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr & collator, String & sort_key_container) const
{
    if (hash.getData().size() != s)
        throw Exception(
            fmt::format("Size of WeakHash32 does not match size of column: column size is {}, hash size is {}", s, hash.getData().size()),
            ErrorCodes::LOGICAL_ERROR);

    WeakHash32 element_hash(1);
    data->updateWeakHash32(element_hash, collator, sort_key_container);
    size_t data_hash = element_hash.getData()[0];

    for (auto & value : hash.getData())
        value = intHashCRC32(data_hash, value);
}

} // namespace DB
