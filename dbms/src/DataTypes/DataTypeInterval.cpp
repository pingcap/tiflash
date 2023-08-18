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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeInterval.h>


namespace DB
{
bool DataTypeInterval::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && kind == static_cast<const DataTypeInterval &>(rhs).kind;
}


void registerDataTypeInterval(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("IntervalSecond", [] {
        return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Second));
    });
    factory.registerSimpleDataType("IntervalMinute", [] {
        return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Minute));
    });
    factory.registerSimpleDataType("IntervalHour", [] {
        return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Hour));
    });
    factory.registerSimpleDataType("IntervalDay", [] {
        return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Day));
    });
    factory.registerSimpleDataType("IntervalWeek", [] {
        return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Week));
    });
    factory.registerSimpleDataType("IntervalMonth", [] {
        return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Month));
    });
    factory.registerSimpleDataType("IntervalYear", [] {
        return DataTypePtr(std::make_shared<DataTypeInterval>(DataTypeInterval::Year));
    });
}

} // namespace DB
