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

#include <Columns/ColumnNothing.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNothing.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>


namespace DB
{
MutableColumnPtr DataTypeNothing::createColumn() const
{
    return ColumnNothing::create(0);
}

void DataTypeNothing::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    size_t size = column.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    for (size_t i = 0; i < limit; ++i)
        ostr.write('0');
}

void DataTypeNothing::deserializeBinaryBulk(
    IColumn & column,
    ReadBuffer & istr,
    size_t limit,
    double,
    const IColumn::Filter *) const
{
    typeid_cast<ColumnNothing &>(column).addSize(istr.tryIgnore(limit));
}

bool DataTypeNothing::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}


void registerDataTypeNothing(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("Nothing", [] { return DataTypePtr(std::make_shared<DataTypeNothing>()); });
}

} // namespace DB
