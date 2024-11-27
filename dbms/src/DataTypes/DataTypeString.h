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
class DataTypeString final : public IDataType
{
public:
    using FieldType = String;
    static constexpr bool is_parametric = false;

    const char * getFamilyName() const override { return "String"; }

    TypeIndex getTypeId() const override { return TypeIndex::String; }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint)
        const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &)
        const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return String(); }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeComparedWithCollation() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isString() const override { return true; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

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

    enum class SerdesFormat
    {
        SizePrefix = 0, // use size-prefix format in serialization/deserialization.
        SeparateSizeAndChars = 1, // seperate sizes and chars in serialization/deserialization.
    };

    DataTypeString(SerdesFormat serdes_fmt_ = SerdesFormat::SeparateSizeAndChars)
        : serdes_fmt(serdes_fmt_)
    {}

private:
    const SerdesFormat serdes_fmt = SerdesFormat::SeparateSizeAndChars;
};

} // namespace DB
