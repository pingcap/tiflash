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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/ProfileEvents.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsGeo.h>
#include <Functions/GeoUtils.h>
#include <Functions/ObjectPool.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <boost_wrapper/geometry.h>

#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

namespace FunctionPointInPolygonDetail
{
template <typename Polygon, typename PointInPolygonImpl>
ColumnPtr callPointInPolygonImplWithPool(const IColumn & x, const IColumn & y, Polygon & polygon)
{
    using Pool = ObjectPoolMap<PointInPolygonImpl, std::string>;
    /// C++11 has thread-safe function-local statics on most modern compilers.
    static Pool known_polygons;

    auto factory = [&polygon]() {
        GeoUtils::normalizePolygon(polygon);
        auto ptr = std::make_unique<PointInPolygonImpl>(polygon);

        /// To allocate memory.
        ptr->init();

        return ptr.release();
    };

    std::string serialized_polygon = GeoUtils::serialize(polygon);
    auto impl = known_polygons.get(serialized_polygon, factory);

    return GeoUtils::pointInPolygon(x, y, *impl);
}

template <typename Polygon, typename PointInPolygonImpl>
ColumnPtr callPointInPolygonImpl(const IColumn & x, const IColumn & y, Polygon & polygon)
{
    PointInPolygonImpl impl(polygon);
    return GeoUtils::pointInPolygon(x, y, impl);
}

} // namespace FunctionPointInPolygonDetail

template <template <typename> typename PointInPolygonImpl, bool use_object_pool = false>
class FunctionPointInPolygon : public IFunction
{
public:
    template <typename Type>
    using Point = boost::geometry::model::d2::point_xy<Type>;
    template <typename Type>
    using Polygon = boost::geometry::model::polygon<Point<Type>, false>;
    template <typename Type>
    using Box = boost::geometry::model::box<Point<Type>>;

    static const char * name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPointInPolygon<PointInPolygonImpl, use_object_pool>>();
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
        {
            throw Exception("Too few arguments", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
        }

        auto get_msg_prefix = [this](size_t i) {
            return "Argument " + toString(i + 1) + " for function " + getName();
        };

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * array = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (array == nullptr && i != 1)
                throw Exception(get_msg_prefix(i) + " must be array of tuples.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * tuple
                = checkAndGetDataType<DataTypeTuple>(array ? array->getNestedType().get() : arguments[i].get());
            if (tuple == nullptr)
                throw Exception(get_msg_prefix(i) + " must contains tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const DataTypes & elements = tuple->getElements();

            if (elements.size() != 2)
                throw Exception(get_msg_prefix(i) + " must have exactly two elements.", ErrorCodes::BAD_ARGUMENTS);

            for (auto j : ext::range(0, elements.size()))
            {
                if (!elements[j]->isNumber())
                {
                    throw Exception(
                        get_msg_prefix(i) + " must contains numeric tuple at position " + toString(j + 1),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * point_col = block.getByPosition(arguments[0]).column.get();
        const auto * const_tuple_col = checkAndGetColumn<ColumnConst>(point_col);
        if (const_tuple_col)
            point_col = &const_tuple_col->getDataColumn();
        const auto * tuple_col = checkAndGetColumn<ColumnTuple>(point_col);

        if (!tuple_col)
        {
            throw Exception(
                "First argument for function " + getName() + " must be constant array of tuples.",
                ErrorCodes::ILLEGAL_COLUMN);
        }

        const Columns & tuple_columns = tuple_col->getColumns();
        const DataTypes & tuple_types
            = typeid_cast<const DataTypeTuple &>(*block.getByPosition(arguments[0]).type).getElements();

        bool use_float64 = checkDataType<DataTypeFloat64>(tuple_types[0].get())
            || checkDataType<DataTypeFloat64>(tuple_types[1].get());

        auto & result_column = block.safeGetByPosition(result).column;

        if (use_float64)
            result_column = executeForType<Float64>(*tuple_columns[0], *tuple_columns[1], block, arguments);
        else
            result_column = executeForType<Float32>(*tuple_columns[0], *tuple_columns[1], block, arguments);

        if (const_tuple_col)
            result_column = ColumnConst::create(result_column, const_tuple_col->size());
    }

private:
    Float64 getCoordinateFromField(const Field & field) const
    {
        switch (field.getType())
        {
        case Field::Types::Float64:
            return field.get<Float64>();
        case Field::Types::Int64:
            return field.get<Int64>();
        case Field::Types::UInt64:
            return field.get<UInt64>();
        default:
        {
            std::string msg = "Expected numeric field, but got ";
            throw Exception(msg + Field::Types::toString(field.getType()), ErrorCodes::LOGICAL_ERROR);
        }
        }
    }

    template <typename Type>
    ColumnPtr executeForType(const IColumn & x, const IColumn & y, Block & block, const ColumnNumbers & arguments) const
    {
        Polygon<Type> polygon;

        auto get_msg_prefix = [this](size_t i) {
            return "Argument " + toString(i + 1) + " for function " + getName();
        };

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * const_col = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[i]).column.get());
            const auto * array_col = const_col ? checkAndGetColumn<ColumnArray>(&const_col->getDataColumn()) : nullptr;
            const auto * tuple_col = array_col ? checkAndGetColumn<ColumnTuple>(&array_col->getData()) : nullptr;

            if (!tuple_col)
                throw Exception(get_msg_prefix(i) + " must be constant array of tuples.", ErrorCodes::ILLEGAL_COLUMN);

            const auto & tuple_columns = tuple_col->getColumns();
            const auto & column_x = tuple_columns[0];
            const auto & column_y = tuple_columns[1];

            if (!polygon.outer().empty())
                polygon.inners().emplace_back();

            auto & container = polygon.outer().empty() ? polygon.outer() : polygon.inners().back();

            auto size = column_x->size();

            if (size == 0)
                throw Exception(get_msg_prefix(i) + " shouldn't be empty.", ErrorCodes::ILLEGAL_COLUMN);

            for (auto j : ext::range(0, size))
            {
                Type x_coord = getCoordinateFromField((*column_x)[j]);
                Type y_coord = getCoordinateFromField((*column_y)[j]);
                container.push_back(Point<Type>(x_coord, y_coord));
            }

            /// Polygon assumed to be closed. Allow user to escape repeating of first point.
            if (!boost::geometry::equals(container.front(), container.back()))
                container.push_back(container.front());
        }

        auto call_impl = use_object_pool
            ? FunctionPointInPolygonDetail::callPointInPolygonImplWithPool<Polygon<Type>, PointInPolygonImpl<Type>>
            : FunctionPointInPolygonDetail::callPointInPolygonImpl<Polygon<Type>, PointInPolygonImpl<Type>>;

        return call_impl(x, y, polygon);
    }
};

template <typename Type>
using Point = boost::geometry::model::d2::point_xy<Type>;

template <typename Type>
using PointInPolygonWithGrid = GeoUtils::PointInPolygonWithGrid<Type>;

template <>
const char * FunctionPointInPolygon<PointInPolygonWithGrid, true>::name = "pointInPolygon";

void registerFunctionsGeo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatCircleDistance>();
    factory.registerFunction<FunctionPointInEllipses>();

    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGrid, true>>();
}
} // namespace DB
