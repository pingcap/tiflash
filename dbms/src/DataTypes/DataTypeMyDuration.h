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

#include <DataTypes/DataTypeNumberBase.h>
#include <fmt/format.h>

namespace DB
{
class DataTypeMyDuration final : public DataTypeNumberBase<Int64>
{
    UInt64 fsp;

public:
    explicit DataTypeMyDuration(UInt64 fsp_ = 0);

    const char * getFamilyName() const override { return "MyDuration"; }

    String getName() const override { return fmt::format("MyDuration({})", fsp); }

    TypeIndex getTypeId() const override { return TypeIndex::MyTime; }

    bool isComparable() const override { return true; };
    bool canBeUsedAsVersion() const override { return true; }
    bool canBeInsideNullable() const override { return true; };
    bool isCategorial() const override { return true; }
    bool isMyTime() const override { return true; };

    bool equals(const IDataType & rhs) const override;

    UInt64 getFsp() const { return fsp; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
};


} // namespace DB
