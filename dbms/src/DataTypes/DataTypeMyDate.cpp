
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMyDate.h>


namespace DB
{

void DataTypeMyDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeMyDateText(static_cast<const ColumnUInt64 &>(column).getData()[row_num], ostr);
}

static void deserializeText(IColumn & column, ReadBuffer & istr)
{
    UInt64 x = 0;
    readMyDateText(x, istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(x);
}

void DataTypeMyDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeMyDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const { deserializeText(column, istr); }

void DataTypeMyDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr);
    writeChar('\'', ostr);
}

void DataTypeMyDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    UInt64 x = 0;
    assertChar('\'', istr);
    readMyDateText(x, istr);
    assertChar('\'', istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(x); /// It's important to do this at the end - for exception safety.
}

void DataTypeMyDate::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeMyDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    UInt64 x = 0;
    assertChar('"', istr);
    readMyDateText(x, istr);
    assertChar('"', istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(x);
}

void DataTypeMyDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeMyDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char /*delimiter*/) const
{
    UInt64 value = 0;
    readCSV(value, istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(value);
}

bool DataTypeMyDate::equals(const IDataType & rhs) const { return typeid(rhs) == typeid(*this); }


void registerDataTypeMyDate(DataTypeFactory & factory)
{
    factory.registerSimpleDataType(
        "MyDate", [] { return DataTypePtr(std::make_shared<DataTypeMyDate>()); }, DataTypeFactory::CaseInsensitive);
}

} // namespace DB
