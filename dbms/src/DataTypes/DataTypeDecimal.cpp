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

#include <Columns/ColumnDecimal.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>

namespace DB
{
template <typename T>
std::string DataTypeDecimal<T>::getName() const
{
    return "Decimal(" + toString(precision) + "," + toString(scale) + ")";
}

template <typename T>
void DataTypeDecimal<T>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    FieldType v = get<DecimalField<T>>(field);
    writeBinary(v, ostr);
}

template <typename T>
void DataTypeDecimal<T>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    T x{};
    readBinary(x, istr);
    field = DecimalField(T(x), scale);
}

template <typename T>
void DataTypeDecimal<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
}

template <typename T>
void DataTypeDecimal<T>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    T x{};
    readBinary(x, istr);
    static_cast<ColumnType &>(column).getData().push_back(FieldType(x));
}

template <typename T>
void DataTypeDecimal<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit)
    const
{
    const typename ColumnType::Container & x = typeid_cast<const ColumnType &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(FieldType) * limit);
}

template <typename T>
void DataTypeDecimal<T>::deserializeBinaryBulk(
    IColumn & column,
    ReadBuffer & istr,
    size_t limit,
    double /*avg_value_size_hint*/) const
{
    typename ColumnType::Container & x = typeid_cast<ColumnType &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char *>(&x[initial_size]), sizeof(FieldType) * limit);
    x.resize(initial_size + size / sizeof(FieldType));
}

template <typename T>
void DataTypeDecimal<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeText(static_cast<const ColumnType &>(column).getData()[row_num], scale, ostr);
}

template <typename T>
void DataTypeDecimal<T>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

template <typename T>
void DataTypeDecimal<T>::readText(T & x, ReadBuffer & istr) const
{
    readDecimalText(x, istr, precision, scale);
}

template <typename T>
void DataTypeDecimal<T>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    T v{};
    this->readText(v, istr);
    static_cast<ColumnType &>(column).getData().push_back(v);
}

template <typename T>
void DataTypeDecimal<T>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

template <typename T>
void DataTypeDecimal<T>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    T v{};
    this->readText(v, istr);
    static_cast<ColumnType &>(column).getData().push_back(v);
}

template <typename T>
void DataTypeDecimal<T>::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    serializeText(column, row_num, ostr);
}

template <typename T>
void DataTypeDecimal<T>::deserializeTextJSON(IColumn &, ReadBuffer &) const
{
    // TODO
    throw Exception("Not yet implemented.");
}

template <typename T>
void DataTypeDecimal<T>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

template <typename T>
void DataTypeDecimal<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char /*delimiter*/) const
{
    T x{};
    readCSVDecimal(x, istr, precision, scale);
    static_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename T>
MutableColumnPtr DataTypeDecimal<T>::createColumn() const
{
    return ColumnType::create(0, scale);
}

template <typename T>
T DataTypeDecimal<T>::getScaleMultiplier(UInt32 scale_) const
{
    typename T::NativeType v = 1;
    for (UInt32 i = 0; i < scale_; i++)
    {
        v = v * 10;
    }
    return v;
}

template <typename T>
T DataTypeDecimal<T>::parseFromString(const String & str) const
{
    ReadBufferFromMemory buf(str.data(), str.size());
    T x(0);
    readDecimalText(x, buf, precision, scale);
    return x;
}

template <typename T>
bool DataTypeDecimal<T>::equals(const IDataType & rhs) const
{
    // make sure rhs has same underlying type with this type.
    if (auto ptr = checkDecimal<T>(rhs))
    {
        return ptr->getScale() == scale && ptr->getPrec() == precision;
    }
    return false;
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
    {
        throw Exception("Decimal data type family must have exactly two arguments: precision and scale");
    }
    const auto * arg0 = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg0 || arg0->value.getType() != Field::Types::UInt64 || arg0->value.get<UInt64>() == 0)
        throw Exception(
            "Decimal data type family must have a number (positive integer) as its argument",
            ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    const auto * arg1 = typeid_cast<const ASTLiteral *>(arguments->children[1].get());
    if (!arg1 || arg1->value.getType() != Field::Types::UInt64)
        throw Exception(
            "Decimal data type family must have a number (positive integer) as its argument",
            ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    return createDecimal(arg0->value.get<UInt64>(), arg1->value.get<UInt64>());
}

void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("Decimal", create, DataTypeFactory::CaseInsensitive);
}

template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;
template class DataTypeDecimal<Decimal256>;

} // namespace DB
