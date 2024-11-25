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

#include <DataTypes/IDataType.h>


namespace DB
{
/** The base class for data types that do not support serialization and deserialization,
  *  but arise only as an intermediate result of the calculations.
  *
  * That is, this class is used just to distinguish the corresponding data type from the others.
  */
class IDataTypeDummy : public IDataType
{
private:
    void throwNoSerialization() const
    {
        throw Exception("Serialization is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

public:
    void serializeBinary(const Field &, WriteBuffer &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &) const override { throwNoSerialization(); }
    void serializeBinaryBulk(const IColumn &, WriteBuffer &, size_t, size_t) const override { throwNoSerialization(); }
    void deserializeBinaryBulk(IColumn &, ReadBuffer &, size_t, double, const IColumn::Filter *) const override
    {
        throwNoSerialization();
    }
    void serializeText(const IColumn &, size_t, WriteBuffer &) const override { throwNoSerialization(); }
    void serializeTextEscaped(const IColumn &, size_t, WriteBuffer &) const override { throwNoSerialization(); }
    void deserializeTextEscaped(IColumn &, ReadBuffer &) const override { throwNoSerialization(); }
    void serializeTextQuoted(const IColumn &, size_t, WriteBuffer &) const override { throwNoSerialization(); }
    void deserializeTextQuoted(IColumn &, ReadBuffer &) const override { throwNoSerialization(); }
    void serializeTextJSON(const IColumn &, size_t, WriteBuffer &, const FormatSettingsJSON &) const override
    {
        throwNoSerialization();
    }
    void deserializeTextJSON(IColumn &, ReadBuffer &) const override { throwNoSerialization(); }

    MutableColumnPtr createColumn() const override
    {
        throw Exception(
            "Method createColumn() is not implemented for data type " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    Field getDefault() const override
    {
        throw Exception(
            "Method getDefault() is not implemented for data type " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertDefaultInto(IColumn &) const override
    {
        throw Exception(
            "Method insertDefaultInto() is not implemented for data type " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    bool haveSubtypes() const override { return false; }
    bool cannotBeStoredInTables() const override { return true; }
};

} // namespace DB
