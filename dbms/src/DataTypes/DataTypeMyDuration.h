#pragma once

#include <DataTypes/DataTypeMyTimeBase.h>
#include <fmt/format.h>

namespace DB
{
class DataTypeMyDuration final : public DataTypeNumberBase<Int64>
{
    int fsp;

public:
    explicit DataTypeMyDuration(int fsp_ = 0);

    const char * getFamilyName() const override { return "MyDuration"; }

    String getName() const override { return fmt::format("MyDuration({})", fsp); }

    TypeIndex getTypeId() const override { return TypeIndex::MyTime; }

    bool isComparable() const override { return true; };
    bool canBeUsedAsVersion() const override { return true; }
    bool canBeInsideNullable() const override { return true; };
    bool isCategorial() const override { return true; }
    bool isMyTime() const override { return true; };

    bool equals(const IDataType & rhs) const override;

    int getFsp() const { return fsp; }
};


} // namespace DB