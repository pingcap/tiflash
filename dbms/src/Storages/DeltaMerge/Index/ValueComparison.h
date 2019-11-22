#pragma once

#include <Common/Decimal.h>
#include <Core/DecimalComparison.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <Functions/FunctionHelpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace DM
{

template <typename T>
std::string compareTypeToString(const T & v)
{
    if constexpr (std::is_same_v<T, Field>)
        return String(v.getTypeName());
    else if constexpr (std::is_same_v<T, std::string>)
        return "string";
    else
        return String(TypeName<T>::get());
}

template <template <typename, typename> class Op>
struct ValueComparision
{
    // Used to check operation type
    using OpInt              = Op<int, int>;
    using EqualsInt          = EqualsOp<int, int>;
    using LessInt            = LessOp<int, int>;
    using LessOrEqualsInt    = LessOrEqualsOp<int, int>;
    using GreaterInt         = GreaterOp<int, int>;
    using GreaterOrEqualsInt = GreaterOrEqualsOp<int, int>;

    /// 1: true, -1: false, 0: cannot compare.
    template <typename Right>
    static int compare(const Field & left_field, const DataTypePtr & right_type, const Right & right)
    {
        bool res = 0;
        bool ok;

        auto LeftGroupType = getGroupType(left_field);
        if (LeftGroupType == Number)
            ok = compare<Number, Right>(left_field, right_type, right, res);
        else if (LeftGroupType == Decimal)
            ok = compare<Decimal, Right>(left_field, right_type, right, res);
        else if (LeftGroupType == String)
            ok = compare<String, Right>(left_field, right_type, right, res);
        else
            ok = compare<Generic, Right>(left_field, right_type, right, res);

        if (ok)
            return res ? 1 : -1;
        else
            return 0;
    }

private:
    enum ValueGroupType
    {
        Number,
        Decimal,
        String,
        Generic,
    };

    static ValueGroupType getGroupType(const Field & field)
    {
        switch (field.getType())
        {
        case Field::Types::Which::UInt64:
        case Field::Types::Which::Int64:
        case Field::Types::Which::Float64:
        case Field::Types::Which::UInt128:
        case Field::Types::Which::Int128:
            return Number;
        case Field::Types::Which::Decimal32:
        case Field::Types::Which::Decimal64:
        case Field::Types::Which::Decimal128:
        case Field::Types::Which::Decimal256:
            return Decimal;
        case Field::Types::Which::String:
            return String;
        default:
            return Generic;
        }
    }

    template <typename T>
    static constexpr ValueGroupType getGroupType()
    {
        if constexpr (DB::IsNumber<T>)
            return Number;
        else if constexpr (
            // clang-format off
            std::is_same_v<T, DecimalField<Decimal32>>
            || std::is_same_v<T, DecimalField<Decimal64>>
            || std::is_same_v<T, DecimalField<Decimal128>>
            || std::is_same_v<T, DecimalField<Decimal256>>
            // clang-format on
        )
            return Decimal;
        else if constexpr (std::is_same_v<T, std::string>)
            return String;
        else
            return Generic;
    }

    /// returns successful or not.
    template <ValueGroupType LeftGroupType, typename Right, ValueGroupType RightGroupType = getGroupType<Right>()>
    static bool compare(const Field & left_field, const DataTypePtr & right_type, const Right & right, bool & res)
    {
        if constexpr (LeftGroupType == Number && RightGroupType == Number)
        {
            if (!(compareNumberLeftType<Field::Types::Which::UInt64, UInt64>(left_field, right, res)
                  || compareNumberLeftType<Field::Types::Which::Int64, Int64>(left_field, right, res)
                  || compareNumberLeftType<Field::Types::Which::Float64, Float64>(left_field, right, res)
                  || compareNumberLeftType<Field::Types::Which::UInt128, UInt128>(left_field, right, res)
                  || compareNumberLeftType<Field::Types::Which::Int128, Int128>(left_field, right, res)))
                throw Exception("Illegal compare " + std::string(left_field.getTypeName()) + " with " + compareTypeToString(right));
            return true;
        }
        else if constexpr (LeftGroupType == Decimal || RightGroupType == Decimal)
        {
            // TODO: support decimal comparison.
            // clang-format off
//            if (!(compareDecimalLeftType<Field::Types::Which::UInt64, UInt64, Number>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Int64, Int64, Number>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Float64, Float64, Number>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::UInt128, UInt128, Number>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Int128, Int128, Number>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Int256, Int256, Number>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Decimal32, DecimalField<Decimal32>, Decimal>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Decimal64, DecimalField<Decimal64>, Decimal>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Decimal128, DecimalField<Decimal128>, Decimal>(left_field, right, res)
//                  || compareDecimalLeftType<Field::Types::Which::Decimal256, DecimalField<Decimal256>, Decimal>(left_field, right, res)))
//                throw Exception("Illegal compare " + std::string(left_field.getTypeName()) + " with " + compareTypeToString(right));
            // clang-format on
            // return true;
            return false;
        }
        else if constexpr (LeftGroupType == String && RightGroupType == String)
        {
            compareStringLeftType(left_field, right, res);
            return true;
        }
        else
        {
            Field right_field;
            if constexpr (std::is_same_v<Field, Right>)
                right_field = right;
            else
                right_field = Field((typename NearestFieldType<Right>::Type)right);
            if (left_field.getType() == right_field.getType())
            {
                if constexpr (std::is_same_v<OpInt, EqualsInt>)
                    res = left_field == right_field;
                else if constexpr (std::is_same_v<OpInt, LessInt>)
                    res = left_field < right_field;
                else if constexpr (std::is_same_v<OpInt, GreaterInt>)
                    res = left_field > right_field;
                else if constexpr (std::is_same_v<OpInt, LessOrEqualsInt>)
                    res = left_field <= right_field;
                else if constexpr (std::is_same_v<OpInt, GreaterOrEqualsInt>)
                    res = left_field >= right_field;
                else
                    throw Exception("Unsupported operation");

                return true;
            }
            if constexpr (LeftGroupType == String)
            {
                if (compareDateOrDateTimeOrEnumWithString(left_field.safeGet<std::string>(), right_type, right, res))
                    return true;
            }
        }
        return false;
    }

    template <Field::Types::Which LeftFieldType, typename Left, typename Right>
    static bool compareNumberLeftType(const Field & left_field, const Right & right, bool & res)
    {
        if (left_field.getType() != LeftFieldType)
            return false;
        auto left = left_field.safeGet<Left>();

        res = Op<Left, Right>::apply(left, right);
        return true;
    }

    template <Field::Types::Which LeftFieldType,
              typename Left,
              ValueGroupType LeftGroupType,
              typename Right,
              ValueGroupType RightGroupType = getGroupType<Right>()>
    static bool compareDecimalLeftType(const Field & left_field, const Right & right, bool & res)
    {
        if (left_field.getType() != LeftFieldType)
            return false;
        if constexpr (LeftGroupType != Decimal && RightGroupType != Decimal)
            return false;

        auto & left        = left_field.safeGet<Left>();
        UInt32 left_scale  = 1;
        UInt32 right_scale = 1;

        if constexpr (LeftGroupType == Decimal)
            left_scale = left.getScale();
        if constexpr (RightGroupType == Decimal)
            right_scale = right.getScale();

        else if constexpr ((LeftGroupType == Number || LeftGroupType == Decimal) && (RightGroupType == Number || RightGroupType == Decimal))
            res = DecimalComparison<Left, Right, Op, true>::compare(left, right, left_scale, right_scale);

        return true;
    }

    static void compareStringLeftType(const Field & left_field, const std::string & right, bool & res)
    {
        auto & left = left_field.safeGet<std::string>();

        if constexpr (std::is_same_v<OpInt, EqualsInt>)
            res = left == right;
        else if constexpr (std::is_same_v<OpInt, LessInt>)
            res = left < right;
        else if constexpr (std::is_same_v<OpInt, GreaterInt>)
            res = left > right;
        else if constexpr (std::is_same_v<OpInt, LessOrEqualsInt>)
            res = left <= right;
        else if constexpr (std::is_same_v<OpInt, GreaterOrEqualsInt>)
            res = left >= right;
        else
            throw Exception("Unsupported operation");
    }

    template <typename Right>
    static bool
    compareDateOrDateTimeOrEnumWithString(const std::string & left, const DataTypePtr & right_type, const Right & right, bool & res)
    {
        const IDataType * number_type = right_type.get();

        bool is_date      = false;
        bool is_date_time = false;
        bool is_uuid      = false;
        bool is_enum8     = false;
        bool is_enum16    = false;

        const auto legal_types = (is_date = checkAndGetDataType<DataTypeDate>(number_type))
            || (is_date_time = checkAndGetDataType<DataTypeDateTime>(number_type))
            || (is_uuid = checkAndGetDataType<DataTypeUUID>(number_type)) || (is_enum8 = checkAndGetDataType<DataTypeEnum8>(number_type))
            || (is_enum16 = checkAndGetDataType<DataTypeEnum16>(number_type));

        if (!legal_types)
            return false;


        if (is_date)
        {
            if constexpr (std::is_same_v<DataTypeDate::FieldType, Right>)
            {
                DayNum_t             date;
                ReadBufferFromMemory in(left.data(), left.size());
                readDateText(date, in);
                if (!in.eof())
                    throw Exception("String is too long for Date: " + left);
                res = Op<DataTypeDate::FieldType, Right>::apply(date, right);
                return true;
            }
        }
        else if (is_date_time)
        {
            if constexpr (std::is_same_v<DataTypeDateTime::FieldType, Right>)
            {
                time_t               date_time;
                ReadBufferFromMemory in(left.data(), left.size());
                readDateTimeText(date_time, in);
                if (!in.eof())
                    throw Exception("String is too long for DateTime: " + left);
                res = Op<DataTypeDateTime::FieldType, Right>::apply(date_time, right);
                return true;
            }
        }
        else if (is_uuid)
        {
            if constexpr (std::is_same_v<UUID, Right>)
            {
                UUID                 uuid;
                ReadBufferFromMemory in(left.data(), left.size());
                readText(uuid, in);
                if (!in.eof())
                    throw Exception("String is too long for UUID: " + left);
                res = Op<UUID, Right>::apply(uuid, right);
                return true;
            }
        }
        else if (is_enum8)
        {
            if constexpr (std::is_same_v<DataTypeEnum8::FieldType, Right>)
            {
                auto type            = static_cast<const DataTypeEnum8 *>(right_type.get());
                auto left_enum_value = type->getValue(left);
                res                  = Op<Int8, Right>::apply(left_enum_value, right);
                return true;
            }
        }
        else if (is_enum16)
        {
            if constexpr (std::is_same_v<DataTypeEnum16::FieldType, Right>)
            {
                auto type            = static_cast<const DataTypeEnum16 *>(right_type.get());
                auto left_enum_value = type->getValue(left);
                res                  = Op<Int16, Right>::apply(left_enum_value, right);
                return true;
            }
        }
        return false;
    }
};

} // namespace DM
} // namespace DB