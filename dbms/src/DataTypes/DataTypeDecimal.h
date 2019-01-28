#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}


class DataTypeDecimal : public IDataType
{
private:
    PrecType precision;
    ScaleType scale;

public:
    using FieldType = Decimal;

    static constexpr bool is_parametric = true;

    DataTypeDecimal() {}

    DataTypeDecimal(size_t precision_, size_t scale_) : precision(precision_), scale(scale_)
    {
        if (precision > decimal_max_prec || scale > precision || scale > decimal_max_scale) {
            throw Exception(getName() + "is out of bound", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
    }

    std::string getName() const override;

    const char * getFamilyName() const override { return "Decimal"; }

    PrecType getPrec() const
    {
        return precision;
    }

    ScaleType getScale() const
    {
        return scale;
    }

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

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override
    {
        return Decimal();
    }

    bool equals(const IDataType & rhs) const override {
        return getName() == rhs.getName();
    }

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; };
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueRepresentedByInteger() const override {return scale == 0;}
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(Decimal); }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
};

}
