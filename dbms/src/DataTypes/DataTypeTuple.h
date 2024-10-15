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

#include <DataTypes/IDataType.h>


namespace DB
{
/** Tuple data type.
  * Used as an intermediate result when evaluating expressions.
  * Also can be used as a column - the result of the query execution.
  *
  * Tuple elements can have names.
  * If an element is unnamed, it will have automatically assigned name like '1', '2', '3' corresponding to its position.
  * Manually assigned names must not begin with digit. Names must be unique.
  *
  * All tuples with same size and types of elements are equivalent for expressions, regardless to names of elements.
  */
class DataTypeTuple final : public IDataType
{
private:
    DataTypes elems;
    Strings names;
    bool have_explicit_names;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeTuple(const DataTypes & elems);
    DataTypeTuple(const DataTypes & elems, const Strings & names);

    std::string getName() const override;
    const char * getFamilyName() const override { return "Tuple"; }

    TypeIndex getTypeId() const override { return TypeIndex::Tuple; }

    bool canBeInsideNullable() const override { return false; }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr) const;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextJSON(
        const IColumn & column,
        size_t row_num,
        WriteBuffer & ostr,
        const FormatSettingsJSON & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    /** Each sub-column in a tuple is serialized in separate stream.
      */
    void enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        const OutputStreamGetter & getter,
        size_t offset,
        size_t limit,
        bool position_independent_encoding,
        SubstreamPath & path) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        const InputStreamGetter & getter,
        size_t limit,
        double avg_value_size_hint,
        bool position_independent_encoding,
        SubstreamPath & path) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return !elems.empty(); }
    bool isComparable() const override;
    bool textCanContainOnlyValidUTF8() const override;
    bool haveMaximumSizeOfValue() const override;
    size_t getMaximumSizeOfValueInMemory() const override;
    size_t getSizeOfValueInMemory() const override;

    const DataTypes & getElements() const { return elems; }
    const Strings & getElementNames() const { return names; }

    size_t getPositionByName(const String & name) const;
};

} // namespace DB
