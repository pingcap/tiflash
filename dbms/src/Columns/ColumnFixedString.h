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
#include <Common/PODArray.h>
#include <common/memcpy.h>
#include <string.h> // memcpy


namespace DB
{
/** A column of values of "fixed-length string" type.
  * If you insert a smaller string, it will be padded with zero bytes.
  */
class ColumnFixedString final : public COWPtrHelper<IColumn, ColumnFixedString>
{
public:
    friend class COWPtrHelper<IColumn, ColumnFixedString>;

    using Chars_t = PaddedPODArray<UInt8>;

private:
    /// Bytes of rows, laid in succession. The strings are stored without a trailing zero byte.
    /** NOTE It is required that the offset and type of chars in the object be the same as that of `data in ColumnUInt8`.
      * Used in `packFixed` function (AggregationCommon.h)
      */
    Chars_t chars;
    /// The size of the rows.
    const size_t n;

    template <bool positive>
    struct less;

    /** Create an empty column of strings of fixed-length `n` */
    explicit ColumnFixedString(size_t n_)
        : n(n_)
    {}

    ColumnFixedString(const ColumnFixedString & src)
        : COWPtrHelper<IColumn, ColumnFixedString>(src)
        , chars(src.chars.begin(), src.chars.end())
        , n(src.n){};


    template <bool has_null>
    void serializeToPosImpl(PaddedPODArray<char *> & pos, size_t start, size_t length) const;

    template <bool has_null>
    void serializeToPosForColumnArrayImpl(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        const IColumn::Offsets & array_offsets) const;

public:
    std::string getName() const override { return "FixedString(" + std::to_string(n) + ")"; }
    const char * getFamilyName() const override { return "FixedString"; }

    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override { return chars.size() / n; }

    size_t byteSize() const override { return chars.size() + sizeof(n); }

    size_t byteSize(size_t /*offset*/, size_t limit) const override { return limit * n; }

    size_t allocatedBytes() const override { return chars.allocated_bytes() + sizeof(n); }

    Field operator[](size_t index) const override
    {
        return String(reinterpret_cast<const char *>(&chars[n * index]), n);
    }

    void get(size_t index, Field & res) const override
    {
        res.assignString(reinterpret_cast<const char *>(&chars[n * index]), n);
    }

    StringRef getDataAt(size_t index) const override { return StringRef(&chars[n * index], n); }

    void insert(const Field & x) override;

    void insertFrom(const IColumn & src_, size_t index) override;

    void insertManyFrom(const IColumn & src_, size_t position, size_t length) override;

    void insertDisjunctFrom(const IColumn & src_, const std::vector<size_t> & position_vec) override;

    void insertData(const char * pos, size_t length) override;

    void insertDefault() override { chars.resize_fill_zero(chars.size() + n); }
    void insertManyDefaults(size_t length) override { chars.resize_fill_zero(chars.size() + n * length); }

    void popBack(size_t elems) override { chars.resize_assume_reserved(chars.size() - n * elems); }

    StringRef serializeValueIntoArena(
        size_t index,
        Arena & arena,
        char const *& begin,
        const TiDB::TiDBCollatorPtr &,
        String &) const override;

    const char * deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &) override;

    void countSerializeByteSizeUnique(PaddedPODArray<size_t> & byte_size, const TiDB::TiDBCollatorPtr & collator)
        const override
    {
        // collator->sortKey() will change the string length, which may exceeds n.
        RUNTIME_CHECK_MSG(
            !collator,
            "{} doesn't support countSerializeByteSizeUnique when collator is not null",
            getName());
        countSerializeByteSize(byte_size);
    }
    void countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const override;

    void countSerializeByteSizeUniqueForColumnArray(
        PaddedPODArray<size_t> & byte_size,
        const IColumn::Offsets & array_offsets,
        const TiDB::TiDBCollatorPtr & collator) const override
    {
        RUNTIME_CHECK_MSG(
            !collator,
            "{} doesn't support countSerializeByteSizeUniqueForColumnArray when collator is not null",
            getName());
        countSerializeByteSizeForColumnArray(byte_size, array_offsets);
    }
    void countSerializeByteSizeForColumnArray(
        PaddedPODArray<size_t> & byte_size,
        const IColumn::Offsets & array_offsets) const override;

    void serializeToPosUnique(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const TiDB::TiDBCollatorPtr & collator,
        String *) const override
    {
        RUNTIME_CHECK_MSG(!collator, "{} doesn't support serializeToPosUnique when collator is not null", getName());
        serializeToPos(pos, start, length, has_null);
    }
    void serializeToPos(PaddedPODArray<char *> & pos, size_t start, size_t length, bool has_null) const override;

    void serializeToPosUniqueForColumnArray(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const IColumn::Offsets & array_offsets,
        const TiDB::TiDBCollatorPtr & collator,
        String *) const override
    {
        RUNTIME_CHECK_MSG(
            !collator,
            "{} doesn't support serializeToPosUniqueForColumnArray when collator is not null",
            getName());
        serializeToPosForColumnArray(pos, start, length, has_null, array_offsets);
    }
    void serializeToPosForColumnArray(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const IColumn::Offsets & array_offsets) const override;

    void deserializeAndInsertFromPosUnique(
        PaddedPODArray<const char *> & pos,
        bool use_nt_align_buffer,
        const TiDB::TiDBCollatorPtr & collator) override
    {
        RUNTIME_CHECK_MSG(
            !collator,
            "{} doesn't support deserializeAndInsertFromPosUnique when collator is not null",
            getName());
        deserializeAndInsertFromPos(pos, use_nt_align_buffer);
    }
    void deserializeAndInsertFromPos(PaddedPODArray<const char *> & pos, bool use_nt_align_buffer) override;

    void deserializeAndInsertFromPosUniqueForColumnArray(
        PaddedPODArray<const char *> & pos,
        const IColumn::Offsets & array_offsets,
        bool use_nt_align_buffer,
        const TiDB::TiDBCollatorPtr & collator) override
    {
        RUNTIME_CHECK_MSG(
            !collator,
            "{} doesn't support deserializeAndInsertFromPosUniqueForColumnArray when collator is not null",
            getName());
        deserializeAndInsertFromPosForColumnArray(pos, array_offsets, use_nt_align_buffer);
    }
    void deserializeAndInsertFromPosForColumnArray(
        PaddedPODArray<const char *> & pos,
        const IColumn::Offsets & array_offsets,
        bool use_nt_align_buffer) override;

    void flushNTAlignBuffer() override {}

    void updateHashWithValue(size_t index, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const override;

    void updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &)
        const override;

    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &, const BlockSelective & selective)
        const override;

    int compareAt(size_t p1, size_t p2, const IColumn & rhs_, int /*nan_direction_hint*/) const override
    {
        const auto & rhs = static_cast<const ColumnFixedString &>(rhs_);
        return memcmp(&chars[p1 * n], &rhs.chars[p2 * n], n);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        return scatterImpl<ColumnFixedString>(num_columns, selector);
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector, const BlockSelective & selective)
        const override
    {
        return scatterImpl<ColumnFixedString>(num_columns, selector, selective);
    }

    void scatterTo(ScatterColumns & columns, const Selector & selector) const override
    {
        scatterToImpl<ColumnFixedString>(columns, selector);
    }
    void scatterTo(ScatterColumns & columns, const Selector & selector, const BlockSelective & selective) const override
    {
        scatterToImpl<ColumnFixedString>(columns, selector, selective);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    void reserve(size_t size) override { chars.reserve(n * size); }
    void reserveAlign(size_t size, size_t alignment) override { chars.reserve(n * size, alignment); }

    void getExtremes(Field & min, Field & max) const override;


    bool canBeInsideNullable() const override { return true; }

    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return n; }
    StringRef getRawData() const override { return StringRef(chars.data(), chars.size()); }

    /// Specialized part of interface, not from IColumn.

    Chars_t & getChars() { return chars; }
    const Chars_t & getChars() const { return chars; }

    size_t getN() const { return n; }

    template <bool selective_block>
    void updateWeakHash32Impl(WeakHash32 & hash, const BlockSelective & selective) const;
};


} // namespace DB
