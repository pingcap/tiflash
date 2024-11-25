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
/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class DataTypeNullable final : public IDataType
{
public:
    static constexpr bool is_parametric = true;

    explicit DataTypeNullable(const DataTypePtr & nested_data_type_);
    std::string getName() const override { return "Nullable(" + nested_data_type->getName() + ")"; }
    const char * getFamilyName() const override { return "Nullable"; }

    TypeIndex getTypeId() const override { return TypeIndex::Nullable; }

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
        SubstreamPath & path,
        const IColumn::Filter * filter) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override
    {
        nested_data_type->serializeBinary(field, ostr);
    }
    void deserializeBinary(Field & field, ReadBuffer & istr) const override
    {
        nested_data_type->deserializeBinary(field, istr);
    }
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &)
        const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return Null(); }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested_data_type->cannotBeStoredInTables(); }
    bool shouldAlignRightInPrettyFormats() const override
    {
        return nested_data_type->shouldAlignRightInPrettyFormats();
    }
    bool textCanContainOnlyValidUTF8() const override { return nested_data_type->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested_data_type->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested_data_type->canBeComparedWithCollation(); }
    bool canBeUsedAsVersion() const override { return false; }
    bool isSummable() const override { return nested_data_type->isSummable(); }
    bool canBeUsedInBooleanContext() const override { return nested_data_type->canBeUsedInBooleanContext(); }
    bool haveMaximumSizeOfValue() const override { return nested_data_type->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override
    {
        return 1 + nested_data_type->getMaximumSizeOfValueInMemory();
    }
    bool isNullable() const override { return true; }
    size_t getSizeOfValueInMemory() const override;
    bool onlyNull() const override;

    const DataTypePtr & getNestedType() const { return nested_data_type; }

private:
    DataTypePtr nested_data_type;
};


DataTypePtr makeNullable(const DataTypePtr & type);
DataTypePtr removeNullable(const DataTypePtr & type);

} // namespace DB
