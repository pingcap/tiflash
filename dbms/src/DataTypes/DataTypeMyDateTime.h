#pragma once

#include <DataTypes/DataTypeMyTimeBase.h>


namespace DB
{

class DataTypeMyDateTime final : public DataTypeMyTimeBase
{
    int fraction;

public:
    DataTypeMyDateTime(int fraction_ = 0);

    const char * getFamilyName() const override { return "MyDateTime"; }

    String getName() const override { return "MyDateTime(" + std::to_string(fraction) + ")"; }

    TypeIndex getTypeId() const override { return TypeIndex::MyDateTime; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    bool equals(const IDataType & rhs) const override;

    int getFraction() const { return fraction; }
};

} // namespace DB
