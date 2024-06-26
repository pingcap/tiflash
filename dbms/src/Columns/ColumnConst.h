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

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}


/** ColumnConst contains another column with single element,
  *  but looks like a column with arbitrary amount of same elements.
  */
class ColumnConst final : public COWPtrHelper<IColumn, ColumnConst>
{
private:
    friend class COWPtrHelper<IColumn, ColumnConst>;

    ColumnPtr data;
    size_t s;

    ColumnConst(const ColumnPtr & data, size_t s);
    ColumnConst(const ColumnConst & src) = default;

public:
    ColumnPtr convertToFullColumn() const;

    ColumnPtr convertToFullColumnIfConst() const override { return convertToFullColumn(); }

    std::string getName() const override { return "Const(" + data->getName() + ")"; }

    const char * getFamilyName() const override { return "Const"; }

    MutableColumnPtr cloneResized(size_t new_size) const override { return ColumnConst::create(data, new_size); }

    size_t size() const override { return s; }

    Field operator[](size_t) const override { return (*data)[0]; }

    void get(size_t, Field & res) const override { data->get(0, res); }

    StringRef getDataAt(size_t) const override { return data->getDataAt(0); }

    StringRef getDataAtWithTerminatingZero(size_t) const override { return data->getDataAtWithTerminatingZero(0); }

    UInt64 get64(size_t) const override { return data->get64(0); }

    UInt64 getUInt(size_t) const override { return data->getUInt(0); }

    Int64 getInt(size_t) const override { return data->getInt(0); }

    bool isNullAt(size_t) const override { return data->isNullAt(0); }

    void insertRangeFrom(const IColumn &, size_t /*start*/, size_t length) override { s += length; }

    void insert(const Field &) override { ++s; }

    void insertData(const char *, size_t) override { ++s; }

    void insertFrom(const IColumn &, size_t) override { ++s; }

    void insertManyFrom(const IColumn &, size_t, size_t length) override { s += length; }

    void insertDisjunctFrom(const IColumn &, const std::vector<size_t> & position_vec) override
    {
        s += position_vec.size();
    }

    void insertMany(const Field &, size_t length) override { s += length; }

    void insertDefault() override { ++s; }

    void insertManyDefaults(size_t length) override { s += length; }

    void popBack(size_t n) override { s -= n; }

    StringRef serializeValueIntoArena(
        size_t,
        Arena & arena,
        char const *& begin,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container) const override
    {
        return data->serializeValueIntoArena(0, arena, begin, collator, sort_key_container);
    }

    const char * deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr & collator) override
    {
        auto & mutable_data = data->assumeMutableRef();
        const auto * res = mutable_data.deserializeAndInsertFromArena(pos, collator);
        mutable_data.popBack(1);
        ++s;
        return res;
    }

    void updateHashWithValue(
        size_t,
        SipHash & hash,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container) const override
    {
        data->updateHashWithValue(0, hash, collator, sort_key_container);
    }

    void updateHashWithValues(
        IColumn::HashValues & hash_values,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container) const override
    {
        for (size_t i = 0; i < s; ++i)
        {
            data->updateHashWithValue(0, hash_values[i], collator, sort_key_container);
        }
    }

    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &, const BlockSelectivePtr &) const override;
    void updateWeakHash32Impl(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    size_t byteSize() const override { return data->byteSize() + sizeof(s); }

    size_t byteSize(size_t /*offset*/, size_t /*limit*/) const override { return byteSize(); }

    size_t allocatedBytes() const override { return data->allocatedBytes() + sizeof(s); }

    int compareAt(size_t, size_t, const IColumn & rhs, int nan_direction_hint) const override
    {
        return data->compareAt(0, 0, *static_cast<const ColumnConst &>(rhs).data, nan_direction_hint);
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector, const BlockSelectivePtr & selective)
        const override;
    MutableColumns scatterImplForColumnConst(ColumnIndex num_columns, const Selector & selector) const;

    void scatterTo(ScatterColumns & columns, const Selector & selector) const override;
    void scatterTo(ScatterColumns & columns, const Selector & selector, const BlockSelectivePtr & selective)
        const override;
    void scatterToImplForColumnConst(ScatterColumns & columns, const Selector & selector) const;
    void gather(ColumnGathererStream &) override
    {
        throw Exception("Cannot gather into constant column " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field & min, Field & max) const override { data->getExtremes(min, max); }

    void forEachSubcolumn(ColumnCallback callback) override { callback(data); }

    bool onlyNull() const override { return data->isNullAt(0); }
    bool isColumnConst() const override { return true; }
    bool isNumeric() const override { return data->isNumeric(); }
    bool isFixedAndContiguous() const override { return data->isFixedAndContiguous(); }
    bool valuesHaveFixedSize() const override { return data->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override { return data->sizeOfValueIfFixed(); }
    StringRef getRawData() const override { return data->getRawData(); }

    /// Not part of the common interface.

    IColumn & getDataColumn() { return data->assumeMutableRef(); }
    const IColumn & getDataColumn() const { return *data; }
    //MutableColumnPtr getDataColumnMutablePtr() { return data; }
    const ColumnPtr & getDataColumnPtr() const { return data; }
    //ColumnPtr & getDataColumnPtr() { return data; }

    Field getField() const { return getDataColumn()[0]; }

    template <typename T>
    T getValue() const
    {
        auto && tmp = getField();
        return std::move(tmp.safeGet<typename NearestFieldType<T>::Type>());
    }
};

} // namespace DB
