#pragma once

#include <DataTypes/DataTypeNumberBase.h>


namespace DB
{

// It stands for mysql datetime/ date/ time types.
class DataTypeMyTimeBase : public DataTypeNumberBase<UInt64>
{
public:
    bool canBeUsedAsVersion() const override { return true; }
    bool isDateOrDateTime() const override { return true; }
    bool isMyDateOrMyDateTime() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
};

} // namespace DB
