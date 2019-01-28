#include<IO/WriteHelpers.h>
#include<IO/ReadHelpers.h>
#include<Core/Field.h>
#include<Columns/IColumn.h>
#include<Columns/ColumnDecimal.h>
#include<Common/typeid_cast.h>
#include<DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include<Parsers/ASTLiteral.h>
#include<Parsers/IAST.h>

namespace DB
{

std::string DataTypeDecimal::getName() const 
{
    return "Decimal(" + toString(precision) + "," + toString(scale) + ")";
}

void DataTypeDecimal::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const Decimal & v = get<const Decimal &>(field);
    writeBinary(v, ostr);
}

void DataTypeDecimal::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    Decimal v;
    readBinary(v, istr);
    field = v;
}

void DataTypeDecimal::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const 
{
    writeBinary(static_cast<const ColumnDecimal &>(column).getData()[row_num], ostr);
}

void DataTypeDecimal::deserializeBinary(IColumn & column, ReadBuffer & istr) const 
{
    Decimal v;
    readBinary(v, istr);
    static_cast<ColumnDecimal&>(column).getData().push_back(v);
}

void DataTypeDecimal::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnDecimal::Container & x = typeid_cast<const ColumnDecimal &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(Decimal) * limit);
}

void DataTypeDecimal::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnDecimal::Container & x = typeid_cast<ColumnDecimal &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(Decimal) * limit);
    x.resize(initial_size + size / sizeof(Decimal));
}

void DataTypeDecimal::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const 
{
    writeString(static_cast<const ColumnDecimal &>(column).getData()[row_num].toString().data(), ostr);
}

void DataTypeDecimal::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeDecimal::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    Decimal v(0, precision, scale);
    readText(v, istr);
    static_cast<ColumnDecimal &>(column).getData().push_back(v);
}

void DataTypeDecimal::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const {
    serializeText(column, row_num, ostr);
}

void DataTypeDecimal::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const {
    Decimal v(0, precision, scale);
    readText(v, istr);
    static_cast<ColumnDecimal &>(column).getData().push_back(v);
}

void DataTypeDecimal::serializeTextJSON(const IColumn &column , size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON & ) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeDecimal::deserializeTextJSON(IColumn & , ReadBuffer & ) const {
    // TODO
    throw Exception("Not yet implemented.");
}

void DataTypeDecimal::serializeTextCSV(const IColumn& column, size_t row_num, WriteBuffer & ostr) const 
{
    serializeText(column, row_num, ostr);
}

void DataTypeDecimal::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char /*delimiter*/) const
{
    Decimal x(0, precision, scale);
    readCSV(x, istr);
    static_cast<ColumnDecimal &>(column).getData().push_back(x);
}

MutableColumnPtr DataTypeDecimal::createColumn() const
{
    return ColumnDecimal::create();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2) {
        throw Exception("wrong arguments");
    }
    const ASTLiteral * arg0 = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg0 || arg0->value.getType() != Field::Types::UInt64 || arg0->value.get<UInt64>() == 0)
        throw Exception("Decimal data type family must have a number (positive integer) as its argument", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    const ASTLiteral * arg1 = typeid_cast<const ASTLiteral *>(arguments->children[1].get());
    if (!arg1 || arg1->value.getType() != Field::Types::UInt64)
        throw Exception("Decimal data type family must have a number (positive integer) as its argument", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    return std::make_shared<DataTypeDecimal>(arg0->value.get<UInt64>(), arg1->value.get<UInt64>());
}

void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("Decimal", create, DataTypeFactory::CaseInsensitive);
}
}
