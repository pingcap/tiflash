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