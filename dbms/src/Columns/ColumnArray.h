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

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Defines.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int NOT_IMPLEMENTED;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

/** A column of array values.
  * In memory, it is represented as one column of a nested type, whose size is equal to the sum of the sizes of all arrays,
  *  and as an array of offsets in it, which allows you to get each element.
  */
class ColumnArray final : public COWPtrHelper<IColumn, ColumnArray>
{
private:
    friend class COWPtrHelper<IColumn, ColumnArray>;

    /** Create an array column with specified values and offsets. */
    ColumnArray(MutableColumnPtr && nested_column, MutableColumnPtr && offsets_column);

    /** Create an empty column of arrays with the type of values as in the column `nested_column` */
    explicit ColumnArray(MutableColumnPtr && nested_column);

    ColumnArray(const ColumnArray &) = default;

    template <bool for_compare>
    void countSerializeByteSizeImpl(PaddedPODArray<size_t> & byte_size, const TiDB::TiDBCollatorPtr & collator) const;

    template <bool has_null, bool for_compare>
    void serializeToPosImpl(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        const TiDB::TiDBCollatorPtr & collator,
        String * sort_key_container) const;

    template <bool for_compare>
    void deserializeAndInsertFromPosImpl(
        PaddedPODArray<char *> & pos,
        bool use_nt_align_buffer,
        const TiDB::TiDBCollatorPtr & collator);

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWPtrHelper<IColumn, ColumnArray>;

    static Ptr create(const ColumnPtr & nested_column, const ColumnPtr & offsets_column)
    {
        return ColumnArray::create(nested_column->assumeMutable(), offsets_column->assumeMutable());
    }

    static Ptr create(const ColumnPtr & nested_column) { return ColumnArray::create(nested_column->assumeMutable()); }

    template <typename... Args, typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args &&... args)
    {
        return Base::create(std::forward<Args>(args)...);
    }

    /** On the index i there is an offset to the beginning of the i + 1 -th element. */
    using ColumnOffsets = ColumnVector<Offset>;

    std::string getName() const override;
    const char * getFamilyName() const override { return "Array"; }
    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    StringRef serializeValueIntoArena(
        size_t n,
        Arena & arena,
        char const *& begin,
        const TiDB::TiDBCollatorPtr &,
        String &) const override;
    const char * deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &) override;

    void countSerializeByteSizeForCmp(PaddedPODArray<size_t> & byte_size, const TiDB::TiDBCollatorPtr & collator)
        const override;
    void countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const override;

    void countSerializeByteSizeForCmpColumnArray(
        PaddedPODArray<size_t> & /* byte_size */,
        const IColumn::Offsets & /* array_offsets */,
        const TiDB::TiDBCollatorPtr & /* collator */) const override
    {
        throw Exception(
            "Method countSerializeByteSizeForCmpColumnArray is not supported for " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }
    void countSerializeByteSizeForColumnArray(
        PaddedPODArray<size_t> & /* byte_size */,
        const IColumn::Offsets & /* array_offsets */) const override
    {
        throw Exception(
            "Method countSerializeByteSizeForColumnArray is not supported for " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    void serializeToPosForCmp(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const TiDB::TiDBCollatorPtr & collator,
        String * sort_key_container) const override;
    void serializeToPos(PaddedPODArray<char *> & pos, size_t start, size_t length, bool has_null) const override;

    void serializeToPosForCmpColumnArray(
        PaddedPODArray<char *> & /* pos */,
        size_t /* start */,
        size_t /* length */,
        bool /* has_null */,
        const IColumn::Offsets & /* array_offsets */,
        const TiDB::TiDBCollatorPtr & /* collator */,
        String * /* sort_key_container */) const override
    {
        throw Exception(
            "Method serializeToPosForCmpColumnArray is not supported for " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }
    void serializeToPosForColumnArray(
        PaddedPODArray<char *> & /* pos */,
        size_t /* start */,
        size_t /* length */,
        bool /* has_null */,
        const IColumn::Offsets & /* array_offsets */) const override
    {
        throw Exception(
            "Method serializeToPosForColumnArray is not supported for " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    void deserializeForCmpAndInsertFromPos(
        PaddedPODArray<char *> & pos,
        bool use_nt_align_buffer,
        const TiDB::TiDBCollatorPtr & collator) override;
    void deserializeAndInsertFromPos(PaddedPODArray<char *> & pos, bool use_nt_align_buffer) override;

    void deserializeForCmpAndInsertFromPosColumnArray(
        PaddedPODArray<char *> & /* pos */,
        const IColumn::Offsets & /* array_offsets */,
        bool /* use_nt_align_buffer */,
        const TiDB::TiDBCollatorPtr & /* collator */) override
    {
        throw Exception(
            "Method deserializeForCmpAndInsertFromPosColumnArray is not supported for " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }
    void deserializeAndInsertFromPosForColumnArray(
        PaddedPODArray<char *> & /* pos */,
        const IColumn::Offsets & /* array_offsets */,
        bool /* use_nt_align_buffer */) override
    {
        throw Exception(
            "Method deserializeAndInsertFromPosForColumnArray is not supported for " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    void flushNTAlignBuffer() override;

    void updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &)
        const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &, const BlockSelective & selective)
        const override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insert(const Field & x) override;
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertManyFrom(const IColumn & src_, size_t n, size_t length) override
    {
        for (size_t i = 0; i < length; ++i)
            insertFrom(src_, n);
    }

    void insertDisjunctFrom(const IColumn & src_, const std::vector<size_t> & position_vec) override
    {
        for (auto position : position_vec)
            insertFrom(src_, position);
    }

    void insertDefault() override;
    void insertManyDefaults(size_t length) override;
    void popBack(size_t n) override;
    /// TODO: If result_size_hint < 0, makes reserve() using size of filtered column, not source column to avoid some OOM issues.
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void reserve(size_t n) override;
    void reserveAlign(size_t n, size_t alignment) override;
    size_t byteSize() const override;
    size_t byteSize(size_t offset, size_t limit) const override;
    size_t allocatedBytes() const override;
    ColumnPtr replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & replicate_offsets)
        const override;
    ColumnPtr convertToFullColumnIfConst() const override;
    void getExtremes(Field & min, Field & max) const override;

    bool hasEqualOffsets(const ColumnArray & other) const;

    /** More efficient methods of manipulation */
    IColumn & getData() { return data->assumeMutableRef(); }
    const IColumn & getData() const { return *data; }

    ColumnOffsets & getOffsetsColumn() { return static_cast<ColumnOffsets &>(offsets->assumeMutableRef()); }
    const ColumnOffsets & getOffsetsColumn() const { return static_cast<const ColumnOffsets &>(*offsets); }

    Offsets & ALWAYS_INLINE getOffsets() { return getOffsetsColumn().getData(); }

    const Offsets & ALWAYS_INLINE getOffsets() const { return getOffsetsColumn().getData(); }

    const ColumnPtr & getDataPtr() const { return data; }
    ColumnPtr & getDataPtr() { return data; }

    const ColumnPtr & getOffsetsPtr() const { return offsets; }
    ColumnPtr & getOffsetsPtr() { return offsets; }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        return scatterImpl<ColumnArray>(num_columns, selector);
    }
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector, const BlockSelective & selective)
        const override
    {
        return scatterImpl<ColumnArray>(num_columns, selector, selective);
    }
    void scatterTo(ScatterColumns & columns, const Selector & selector) const override
    {
        scatterToImpl<ColumnArray>(columns, selector);
    }
    void scatterTo(ScatterColumns & columns, const Selector & selector, const BlockSelective & selective) const override
    {
        scatterToImpl<ColumnArray>(columns, selector, selective);
    }
    void gather(ColumnGathererStream & gatherer_stream) override;

    void forEachSubcolumn(ColumnCallback callback) override
    {
        callback(offsets);
        callback(data);
    }

    bool canBeInsideNullable() const override { return true; }

    bool decodeTiDBRowV2Datum(size_t cursor, const String & raw_value, size_t /* length */, bool /* force_decode */)
        override;

    void insertFromDatumData(const char * data, size_t length) override;

    std::pair<UInt32, StringRef> getElementRef(size_t element_idx) const;

    size_t ALWAYS_INLINE sizeAt(ssize_t i) const { return (getOffsets()[i] - getOffsets()[i - 1]); }

private:
    ColumnPtr data;
    ColumnPtr offsets;

    size_t ALWAYS_INLINE offsetAt(size_t i) const { return i == 0 ? 0 : getOffsets()[i - 1]; }

    /// Multiply values if the nested column is ColumnVector<T>.
    template <typename T>
    ColumnPtr replicateNumber(const Offsets & replicate_offsets) const;

    /// Multiply the values if the nested column is ColumnString. The code is too complicated.
    ColumnPtr replicateString(const Offsets & replicate_offsets) const;

    /** Non-constant arrays of constant values are quite rare.
      * Most functions can not work with them, and does not create such columns as a result.
      * An exception is the function `replicate`(see FunctionsMiscellaneous.h), which has service meaning for the implementation of lambda functions.
      * Only for its sake is the implementation of the `replicate` method for ColumnArray(ColumnConst).
      */
    ColumnPtr replicateConst(const Offsets & replicate_offsets) const;

    /** The following is done by simply replicating of nested columns.
      */
    ColumnPtr replicateTuple(const Offsets & replicate_offsets) const;
    ColumnPtr replicateNullable(const Offsets & replicate_offsets) const;
    ColumnPtr replicateGeneric(const Offsets & replicate_offsets) const;


    /// Specializations for the filter function.
    template <typename T>
    ColumnPtr filterNumber(const Filter & filt, ssize_t result_size_hint) const;

    ColumnPtr filterString(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterTuple(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterNullable(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterGeneric(const Filter & filt, ssize_t result_size_hint) const;

    template <bool selective_block>
    void updateWeakHash32Impl(
        WeakHash32 & hash,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container,
        const BlockSelective & selective) const;
};


} // namespace DB
