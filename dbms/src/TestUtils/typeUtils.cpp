// Copyright 2023 PingCAP, Ltd.
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

#include <TestUtils/typeUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Types.h>

namespace DB::tests
{
TiDB::TP dataTypeToTP(const DataTypePtr & type)
{
    switch (removeNullable(type)->getTypeId())
    {
    case TypeIndex::UInt8:
    case TypeIndex::Int8:
        return TiDB::TP::TypeTiny;
    case TypeIndex::UInt16:
    case TypeIndex::Int16:
        return TiDB::TP::TypeShort;
    case TypeIndex::UInt32:
    case TypeIndex::Int32:
        return TiDB::TP::TypeLong;
    case TypeIndex::UInt64:
    case TypeIndex::Int64:
        return TiDB::TP::TypeLongLong;
    case TypeIndex::String:
        return TiDB::TP::TypeString;
    case TypeIndex::Float32:
        return TiDB::TP::TypeFloat;
    case TypeIndex::Float64:
        return TiDB::TP::TypeDouble;
    case TypeIndex::Date:
    case TypeIndex::MyDate:
        return TiDB::TP::TypeDate;
    case TypeIndex::DateTime:
    case TypeIndex::MyDateTime:
        return TiDB::TP::TypeDatetime;
    case TypeIndex::MyTimeStamp:
        return TiDB::TP::TypeTimestamp;
    case TypeIndex::MyTime:
        return TiDB::TP::TypeTime;
    case TypeIndex::Decimal32:
    case TypeIndex::Decimal64:
    case TypeIndex::Decimal128:
    case TypeIndex::Decimal256:
        return TiDB::TP::TypeNewDecimal;
    case TypeIndex::Enum8:
    case TypeIndex::Enum16:
        return TiDB::TP::TypeEnum;
    default:
        throw Exception("Unsupport type");
    }
}
} // namespace DB::tests