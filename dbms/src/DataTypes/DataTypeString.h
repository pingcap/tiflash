#pragma once

#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>

#include <ostream>
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
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return String(); }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; };
    bool canBeComparedWithCollation() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isString() const override { return true; };
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
};

#ifdef __x86_64__
#define DECLARE_DESERIALIZE_BIN_AVX2(UNROLL)             \
    extern void deserializeBinaryAVX2##ByUnRoll##UNROLL( \
        ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit);
DECLARE_DESERIALIZE_BIN_AVX2(4)
DECLARE_DESERIALIZE_BIN_AVX2(3)
DECLARE_DESERIALIZE_BIN_AVX2(2)
DECLARE_DESERIALIZE_BIN_AVX2(1)
#undef DECLARE_DESERIALIZE_BIN_AVX2
#endif
} // namespace DB
