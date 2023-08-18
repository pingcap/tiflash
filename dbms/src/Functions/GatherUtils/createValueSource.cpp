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

#include <Core/TypeListNumber.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Sources.h>

namespace DB::GatherUtils
{
/// Creates IValueSource from Column

template <typename... Types>
struct ValueSourceCreator;

template <typename Type, typename... Types>
struct ValueSourceCreator<Type, Types...>
{
    static std::unique_ptr<IValueSource> create(
        const IColumn & col,
        const NullMap * null_map,
        bool is_const,
        size_t total_rows)
    {
        if (auto column_vector = typeid_cast<const ColumnVector<Type> *>(&col))
        {
            if (null_map)
            {
                if (is_const)
                    return std::make_unique<ConstSource<NullableValueSource<NumericValueSource<Type>>>>(
                        *column_vector,
                        *null_map,
                        total_rows);
                return std::make_unique<NullableValueSource<NumericValueSource<Type>>>(*column_vector, *null_map);
            }
            if (is_const)
                return std::make_unique<ConstSource<NumericValueSource<Type>>>(*column_vector, total_rows);
            return std::make_unique<NumericValueSource<Type>>(*column_vector);
        }

        return ValueSourceCreator<Types...>::create(col, null_map, is_const, total_rows);
    }
};

template <>
struct ValueSourceCreator<>
{
    static std::unique_ptr<IValueSource> create(
        const IColumn & col,
        const NullMap * null_map,
        bool is_const,
        size_t total_rows)
    {
        if (null_map)
        {
            if (is_const)
                return std::make_unique<ConstSource<NullableValueSource<GenericValueSource>>>(
                    col,
                    *null_map,
                    total_rows);
            return std::make_unique<NullableValueSource<GenericValueSource>>(col, *null_map);
        }
        if (is_const)
            return std::make_unique<ConstSource<GenericValueSource>>(col, total_rows);
        return std::make_unique<GenericValueSource>(col);
    }
};

std::unique_ptr<IValueSource> createValueSource(const IColumn & col, bool is_const, size_t total_rows)
{
    using Creator = typename ApplyTypeListForClass<ValueSourceCreator, TypeListNumbers>::Type;
    if (auto column_nullable = typeid_cast<const ColumnNullable *>(&col))
    {
        return Creator::create(
            column_nullable->getNestedColumn(),
            &column_nullable->getNullMapData(),
            is_const,
            total_rows);
    }
    return Creator::create(col, nullptr, is_const, total_rows);
}

} // namespace DB::GatherUtils
