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
/// Creates IArraySource from ColumnArray

template <typename... Types>
struct ArraySourceCreator;

template <typename Type, typename... Types>
struct ArraySourceCreator<Type, Types...>
{
    static std::unique_ptr<IArraySource> create(
        const ColumnArray & col,
        const NullMap * null_map,
        bool is_const,
        size_t total_rows)
    {
        if (typeid_cast<const ColumnVector<Type> *>(&col.getData()))
        {
            if (null_map)
            {
                if (is_const)
                    return std::make_unique<ConstSource<NullableArraySource<NumericArraySource<Type>>>>(
                        col,
                        *null_map,
                        total_rows);
                return std::make_unique<NullableArraySource<NumericArraySource<Type>>>(col, *null_map);
            }
            if (is_const)
                return std::make_unique<ConstSource<NumericArraySource<Type>>>(col, total_rows);
            return std::make_unique<NumericArraySource<Type>>(col);
        }

        return ArraySourceCreator<Types...>::create(col, null_map, is_const, total_rows);
    }
};

template <>
struct ArraySourceCreator<>
{
    static std::unique_ptr<IArraySource> create(
        const ColumnArray & col,
        const NullMap * null_map,
        bool is_const,
        size_t total_rows)
    {
        if (null_map)
        {
            if (is_const)
                return std::make_unique<ConstSource<NullableArraySource<GenericArraySource>>>(
                    col,
                    *null_map,
                    total_rows);
            return std::make_unique<NullableArraySource<GenericArraySource>>(col, *null_map);
        }
        if (is_const)
            return std::make_unique<ConstSource<GenericArraySource>>(col, total_rows);
        return std::make_unique<GenericArraySource>(col);
    }
};

std::unique_ptr<IArraySource> createArraySource(const ColumnArray & col, bool is_const, size_t total_rows)
{
    using Creator = typename ApplyTypeListForClass<ArraySourceCreator, TypeListNumbers>::Type;
    if (auto column_nullable = typeid_cast<const ColumnNullable *>(&col.getData()))
    {
        auto column = ColumnArray::create(column_nullable->getNestedColumnPtr(), col.getOffsetsPtr());
        return Creator::create(*column, &column_nullable->getNullMapData(), is_const, total_rows);
    }
    return Creator::create(col, nullptr, is_const, total_rows);
}
} // namespace DB::GatherUtils
