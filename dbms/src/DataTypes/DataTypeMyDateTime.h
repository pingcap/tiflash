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

#include <DataTypes/DataTypeMyTimeBase.h>


namespace DB
{
class DataTypeMyDateTime final : public DataTypeMyTimeBase
{
    int fraction;

public:
    explicit DataTypeMyDateTime(int fraction_ = 0);

    const char * getFamilyName() const override { return "MyDateTime"; }

    String getName() const override { return "MyDateTime(" + std::to_string(fraction) + ")"; }

    TypeIndex getTypeId() const override { return TypeIndex::MyDateTime; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &)
        const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    bool equals(const IDataType & rhs) const override;

    int getFraction() const { return fraction; }
};

} // namespace DB
