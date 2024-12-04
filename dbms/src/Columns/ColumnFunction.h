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
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

namespace DB
{
class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/** A column containing a lambda expression.
  * Behaves like a constant-column. Contains an expression, but not input or output data.
  */
class ColumnFunction final : public COWPtrHelper<IColumn, ColumnFunction>
{
private:
    friend class COWPtrHelper<IColumn, ColumnFunction>;

    ColumnFunction(size_t size, FunctionBasePtr function, const ColumnsWithTypeAndName & columns_to_capture);

public:
    const char * getFamilyName() const override { return "Function"; }

    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override { return column_size; }

    ColumnPtr cut(size_t start, size_t length) const override;
    ColumnPtr replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const override;
    ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    void insertDefault() override;

    void insertManyDefaults(size_t length) override
    {
        for (size_t i = 0; i < length; ++i)
            insertDefault();
    }
    void popBack(size_t n) override;
    ScatterColumns scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const override;
    ScatterColumns scatter(
        IColumn::ColumnIndex num_columns,
        const IColumn::Selector & selector,
        const BlockSelective & selective) const override;
    void scatterTo(ScatterColumns & columns, const Selector & selector) const override;
    void scatterTo(ScatterColumns &, const Selector &, const BlockSelective &) const override;

    void getExtremes(Field &, Field &) const override {}

    size_t byteSize() const override;
    size_t byteSize(size_t /*offset*/, size_t limit) const override;
    size_t allocatedBytes() const override;

    void appendArguments(const ColumnsWithTypeAndName & columns);
    ColumnWithTypeAndName reduce() const;

    Field operator[](size_t) const override
    {
        throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void get(size_t, Field &) const override
    {
        throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    StringRef getDataAt(size_t) const override
    {
        throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void insert(const Field &) override
    {
        throw Exception("Cannot get insert into " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertRangeFrom(const IColumn &, size_t, size_t) override
    {
        throw Exception("Cannot insert into " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertManyFrom(const IColumn &, size_t, size_t) override
    {
        throw Exception("Cannot insert into " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertSelectiveFrom(const IColumn &, const Offsets &) override
    {
        throw Exception("Cannot insert into " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertData(const char *, size_t) override
    {
        throw Exception("Cannot insert into " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    StringRef serializeValueIntoArena(size_t, Arena &, char const *&, const TiDB::TiDBCollatorPtr &, String &)
        const override
    {
        throw Exception("Cannot serialize from " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    const char * deserializeAndInsertFromArena(const char *, const TiDB::TiDBCollatorPtr &) override
    {
        throw Exception("Cannot deserialize to " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void countSerializeByteSize(PaddedPODArray<size_t> & /* byte_size */) const override
    {
        throw Exception("Method countSerializeByteSize is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
    void countSerializeByteSizeForColumnArray(
        PaddedPODArray<size_t> & /* byte_size */,
        const IColumn::Offsets & /* offsets */) const override
    {
        throw Exception(
            "Method countSerializeByteSizeForColumnArray is not supported for " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    void serializeToPos(
        PaddedPODArray<char *> & /* pos */,
        size_t /* start */,
        size_t /* length */,
        bool /* has_null */) const override
    {
        throw Exception("Method serializeToPos is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
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

    void deserializeAndInsertFromPos(PaddedPODArray<char *> & /* pos */, bool /* use_nt_align_buffer */) override
    {
        throw Exception(
            "Method deserializeAndInsertFromPos is not supported for " + getName(),
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

    void flushNTAlignBuffer() override
    {
        throw Exception("Method flushNTAlignBuffer is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateHashWithValue(size_t, SipHash &, const TiDB::TiDBCollatorPtr &, String &) const override
    {
        throw Exception("updateHashWithValue is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateHashWithValues(IColumn::HashValues &, const TiDB::TiDBCollatorPtr &, String &) const override
    {
        throw Exception("updateHashWithValues is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateWeakHash32(WeakHash32 &, const TiDB::TiDBCollatorPtr &, String &) const override
    {
        throw Exception("updateWeakHash32 is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateWeakHash32(WeakHash32 &, const TiDB::TiDBCollatorPtr &, String &, const BlockSelective &) const override
    {
        throw Exception("updateWeakHash32 is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    int compareAt(size_t, size_t, const IColumn &, int) const override
    {
        throw Exception("compareAt is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void getPermutation(bool, size_t, int, Permutation &) const override
    {
        throw Exception("getPermutation is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void gather(ColumnGathererStream &) override
    {
        throw Exception("Method gather is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    size_t column_size;
    FunctionBasePtr function;
    ColumnsWithTypeAndName captured_columns;

    void appendArgument(const ColumnWithTypeAndName & column);
};

} // namespace DB
