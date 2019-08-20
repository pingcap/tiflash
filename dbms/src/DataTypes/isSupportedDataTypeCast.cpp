#include <DataTypes/isSupportedDataTypeCast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

bool isSupportedDataTypeCast(const DataTypePtr &from, const DataTypePtr &to)
{
    try
    {
        return !isLossyCast(from, to);
    }
    catch (DB::Exception & e)
    {
        return false;
    }
}

bool isLossyCast(const DataTypePtr &from, const DataTypePtr &to)
{
    assert(from != nullptr && to != nullptr);
    /// `to` is equal to `from`
    if (to->equals(*from))
    {
        return false;
    }

    /// For Nullable
    {
        bool has_nullable = false;
        DataTypePtr from_not_null;
        if (const DataTypeNullable * type_nullable = typeid_cast<const DataTypeNullable *>(from.get()))
        {
            has_nullable = true;
            from_not_null = type_nullable->getNestedType();
        }
        else
        {
            from_not_null = from;
        }

        DataTypePtr to_not_null;
        if (const DataTypeNullable * type_nullable = typeid_cast<const DataTypeNullable *>(to.get()))
        {
            has_nullable = true;
            to_not_null = type_nullable->getNestedType();
        }
        else
        {
            to_not_null = to;
        }

        if (has_nullable)
            return isLossyCast(from_not_null, to_not_null);
    }

    /// For numeric types (integer, floats)
    if (from->isNumber() && to->isNumber())
    {
        /// int <-> float, or float32 <-> float64, is not supported
        if (!from->isInteger() || !to->isInteger())
        {
            return true;
        }
        /// Change from signed to unsigned, or vice versa, is lossy
        // use xor(^)
        if (from->isUnsignedInteger() ^ to->isUnsignedInteger())
        {
            return true;
        }

        /// Both signed or unsigned, compare the sizeof(Type)
        size_t from_sz = from->getSizeOfValueInMemory();
        size_t to_sz = to->getSizeOfValueInMemory();
        return from_sz > to_sz;
    }

    /// For String / FixedString
    if (from->isStringOrFixedString() && to->isStringOrFixedString())
    {
        size_t from_sz = std::numeric_limits<size_t>::max();
        if (const DataTypeFixedString * type_fixed_str = typeid_cast<const DataTypeFixedString *>(from.get()))
            from_sz = type_fixed_str->getN();
        size_t to_sz = std::numeric_limits<size_t>::max();
        if (const DataTypeFixedString * type_fixed_str = typeid_cast<const DataTypeFixedString *>(to.get()))
            to_sz = type_fixed_str->getN();
        return from_sz > to_sz;
    }

    /// For Date and DateTime, not supported
    if (from->isDateOrDateTime() || to->isDateOrDateTime())
    {
        return true;
    }

    // TODO enums, set?

    /// some DataTypes that support in ClickHouse but not in TiDB

    // Cast to Nothing / from Nothing is lossy
    if (typeid_cast<const DataTypeNothing *>(from.get()) || typeid_cast<const DataTypeNothing *>(to.get()))
    {
        return true;
    }

    // Cast to Array / from Array is not supported
    if (typeid_cast<const DataTypeArray *>(from.get()) || typeid_cast<const DataTypeArray *>(to.get()))
    {
        throw Exception("Cast from " + from->getName() + " to " + to->getName() + " is not supported.", ErrorCodes::NOT_IMPLEMENTED);
    }

    // Cast to Tuple / from Tuple is not supported
    if (typeid_cast<const DataTypeTuple *>(from.get()) || typeid_cast<const DataTypeTuple *>(to.get()))
    {
        throw Exception("Cast from " + from->getName() + " to " + to->getName() + " is not supported.", ErrorCodes::NOT_IMPLEMENTED);
    }

    throw Exception("Cast from " + from->getName() + " to " + to->getName() + " is not supported.", ErrorCodes::NOT_IMPLEMENTED);
}

} // namespace DB
