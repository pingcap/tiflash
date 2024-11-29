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

#define MAX_FIXEDSTRING_SIZE 0xFFFFFF


namespace DB
{
namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
}


class DataTypeFixedString final : public IDataType
{
private:
    size_t n;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeFixedString(size_t n_)
        : n(n_)
    {
        if (n == 0)
            throw Exception("FixedString size must be positive", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (n > MAX_FIXEDSTRING_SIZE)
            throw Exception("FixedString size is too large", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    std::string getName() const override;

    const char * getFamilyName() const override { return "FixedString"; }

    TypeIndex getTypeId() const override { return TypeIndex::FixedString; }

    size_t getN() const { return n; }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(
        IColumn & column,
        ReadBuffer & istr,
        size_t limit,
        double avg_value_size_hint,
        const IColumn::Filter * filter) const override;

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

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isFixedString() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return n; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
};

} // namespace DB
