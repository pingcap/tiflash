// Copyright 2025 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/DeltaMerge/Filter/IntegerSet.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader.h>

#include <algorithm>
#include <limits>
#include <vector>


namespace DB::DM
{

IntegerSetPtr IntegerSet::createValueSet(const DataTypePtr & type, const Fields & values)
{
    auto type_id = removeNullable(type)->getTypeId();
    switch (type_id)
    {
    case TypeIndex::UInt8:
        return std::make_shared<ValueSet<UInt8>>(values);
    case TypeIndex::UInt16:
        return std::make_shared<ValueSet<UInt16>>(values);
    case TypeIndex::UInt32:
        return std::make_shared<ValueSet<UInt32>>(values);
    case TypeIndex::UInt64:
        return std::make_shared<ValueSet<UInt64>>(values);
    case TypeIndex::Int8:
        return std::make_shared<ValueSet<Int8>>(values);
    case TypeIndex::Int16:
        return std::make_shared<ValueSet<Int16>>(values);
    case TypeIndex::Int32:
        return std::make_shared<ValueSet<Int32>>(values);
    case TypeIndex::Int64:
        return std::make_shared<ValueSet<Int64>>(values);
    case TypeIndex::Date:
        return std::make_shared<ValueSet<UInt16>>(values);
    case TypeIndex::DateTime:
        return std::make_shared<ValueSet<UInt32>>(values);
    case TypeIndex::Enum8:
        return std::make_shared<ValueSet<Int8>>(values);
    case TypeIndex::Enum16:
        return std::make_shared<ValueSet<Int16>>(values);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::MyTimeStamp:
        return std::make_shared<ValueSet<UInt64>>(values);
    case TypeIndex::MyTime:
        return std::make_shared<ValueSet<Int64>>(values);
    default:
        return nullptr;
    }
}

IntegerSetPtr IntegerSet::createLessRangeSet(const DataTypePtr & type, const Field & max, bool not_included)
{
    auto func = []<typename T>(const Field & max, bool not_included) -> IntegerSetPtr {
        auto max_value = max.get<T>();
        if (max_value == std::numeric_limits<T>::min() && not_included)
            return EmptySet::instance();
        else if (max_value == std::numeric_limits<T>::min() && !not_included)
            return std::make_shared<ValueSet<T>>(std::set<T>{std::numeric_limits<T>::min()});
        else
            return std::make_shared<RangeSet<T>>(std::numeric_limits<T>::min(), max_value - not_included);
    };

    auto type_id = removeNullable(type)->getTypeId();
    switch (type_id)
    {
    case TypeIndex::UInt8:
        return func.template operator()<UInt8>(max, not_included);
    case TypeIndex::UInt16:
        return func.template operator()<UInt16>(max, not_included);
    case TypeIndex::UInt32:
        return func.template operator()<UInt32>(max, not_included);
    case TypeIndex::UInt64:
        return func.template operator()<UInt64>(max, not_included);
    case TypeIndex::Int8:
        return func.template operator()<Int8>(max, not_included);
    case TypeIndex::Int16:
        return func.template operator()<Int16>(max, not_included);
    case TypeIndex::Int32:
        return func.template operator()<Int32>(max, not_included);
    case TypeIndex::Int64:
        return func.template operator()<Int64>(max, not_included);
    case TypeIndex::Date:
        return func.template operator()<UInt16>(max, not_included);
    case TypeIndex::DateTime:
        return func.template operator()<UInt32>(max, not_included);
    case TypeIndex::Enum8:
        return func.template operator()<Int8>(max, not_included);
    case TypeIndex::Enum16:
        return func.template operator()<Int16>(max, not_included);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::MyTimeStamp:
        return func.template operator()<UInt64>(max, not_included);
    case TypeIndex::MyTime:
        return func.template operator()<Int64>(max, not_included);
    default:
        return nullptr;
    }
}

IntegerSetPtr IntegerSet::createGreaterRangeSet(const DataTypePtr & type, const Field & min, bool not_included)
{
    auto func = []<typename T>(const Field & min, bool not_included) -> IntegerSetPtr {
        auto min_value = min.get<T>();
        if (min_value == std::numeric_limits<T>::max() && not_included)
            return EmptySet::instance();
        else if (min_value == std::numeric_limits<T>::max() && !not_included)
            return std::make_shared<ValueSet<T>>(std::set<T>{std::numeric_limits<T>::max()});
        else
            return std::make_shared<RangeSet<T>>(min_value + not_included, std::numeric_limits<T>::max());
    };

    auto type_id = removeNullable(type)->getTypeId();
    switch (type_id)
    {
    case TypeIndex::UInt8:
        return func.template operator()<UInt8>(min, not_included);
    case TypeIndex::UInt16:
        return func.template operator()<UInt16>(min, not_included);
    case TypeIndex::UInt32:
        return func.template operator()<UInt32>(min, not_included);
    case TypeIndex::UInt64:
        return func.template operator()<UInt64>(min, not_included);
    case TypeIndex::Int8:
        return func.template operator()<Int8>(min, not_included);
    case TypeIndex::Int16:
        return func.template operator()<Int16>(min, not_included);
    case TypeIndex::Int32:
        return func.template operator()<Int32>(min, not_included);
    case TypeIndex::Int64:
        return func.template operator()<Int64>(min, not_included);
    case TypeIndex::Date:
        return func.template operator()<UInt16>(min, not_included);
    case TypeIndex::DateTime:
        return func.template operator()<UInt32>(min, not_included);
    case TypeIndex::Enum8:
        return func.template operator()<Int8>(min, not_included);
    case TypeIndex::Enum16:
        return func.template operator()<Int16>(min, not_included);
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
    case TypeIndex::MyTimeStamp:
        return func.template operator()<UInt64>(min, not_included);
    case TypeIndex::MyTime:
        return func.template operator()<Int64>(min, not_included);
    default:
        return nullptr;
    }
}

IntegerSetPtr EmptySet::invert() const
{
    return AllSet::instance();
}

template <typename T>
IntegerSetPtr ValueSet<T>::intersectWith(const IntegerSetPtr & other)
{
    auto type = other->getType();
    switch (type)
    {
    case SetType::Value:
        return ValueSet<T>::intersectWithValueSet(this->shared_from_this(), other);
    case SetType::Range:
        return ValueSet<T>::intersectWithRangeSet(other, this->shared_from_this());
    case SetType::Composite:
        return ValueSet<T>::intersectWithCompositeSet(other, this->shared_from_this());
    case SetType::All:
    case SetType::Empty:
        return other->intersectWith(this->shared_from_this());
    }
}

template <typename T>
IntegerSetPtr ValueSet<T>::intersectWithValueSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs)
{
    RUNTIME_CHECK(lhs->getType() == SetType::Value);
    RUNTIME_CHECK(rhs->getType() == SetType::Value);

    auto left = std::dynamic_pointer_cast<ValueSet<T>>(lhs);
    auto right = std::dynamic_pointer_cast<ValueSet<T>>(rhs);

    ValueSet<T> result;
    std::set_intersection(
        left->values.begin(),
        left->values.end(),
        right->values.begin(),
        right->values.end(),
        std::inserter(result.values, result.values.begin()));

    return result.values.empty() ? EmptySet::instance() : std::make_shared<ValueSet<T>>(result);
}

template <typename T>
IntegerSetPtr ValueSet<T>::intersectWithRangeSet(const IntegerSetPtr & range_set, const IntegerSetPtr & value_set)
{
    RUNTIME_CHECK(range_set->getType() == SetType::Range);
    RUNTIME_CHECK(value_set->getType() == SetType::Value);

    auto range = std::dynamic_pointer_cast<RangeSet<T>>(range_set);
    auto value = std::dynamic_pointer_cast<ValueSet<T>>(value_set);

    ValueSet<T> result;
    for (const auto v : value->values)
    {
        if (range->start <= v && v <= range->end)
            result.values.insert(v);
    }

    return result.values.empty() ? EmptySet::instance() : std::make_shared<ValueSet<T>>(result);
}

template <typename T>
IntegerSetPtr ValueSet<T>::intersectWithCompositeSet(
    const IntegerSetPtr & composite_set,
    const IntegerSetPtr & value_set)
{
    RUNTIME_CHECK(composite_set->getType() == SetType::Composite);
    RUNTIME_CHECK(value_set->getType() == SetType::Value);

    auto composite = std::dynamic_pointer_cast<CompositeSet<T>>(composite_set);
    auto value = std::dynamic_pointer_cast<ValueSet<T>>(value_set);

    CompositeSet<T> result;
    for (const auto & set : composite->sets)
    {
        if (auto new_set = set->intersectWith(value); new_set)
            result.sets.push_back(new_set);
    }

    if (result.sets.empty())
        return EmptySet::instance();
    if (result.sets.size() == 1)
        return result.sets.front();

    // Combine all sets
    ValueSet<T> combined;
    for (const auto & set : result.sets)
    {
        RUNTIME_CHECK(set->getType() == SetType::Value);

        auto value_set = std::dynamic_pointer_cast<ValueSet<T>>(set);
        combined.values.insert(value_set->values.begin(), value_set->values.end());
    }
    return std::make_shared<ValueSet<T>>(combined);
}

template <typename T>
IntegerSetPtr ValueSet<T>::unionWith(const IntegerSetPtr & other)
{
    auto type = other->getType();
    switch (type)
    {
    case SetType::Value:
        return ValueSet<T>::unionWithValueSet(this->shared_from_this(), other);
    case SetType::Range:
        return ValueSet<T>::unionWithRangeSet(other, this->shared_from_this());
    case SetType::Composite:
        return ValueSet<T>::unionWithCompositeSet(other, this->shared_from_this());
    case SetType::All:
    case SetType::Empty:
        return other->unionWith(this->shared_from_this());
    }
}

template <typename T>
IntegerSetPtr ValueSet<T>::unionWithValueSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs)
{
    RUNTIME_CHECK(lhs->getType() == SetType::Value);
    RUNTIME_CHECK(rhs->getType() == SetType::Value);

    auto left = std::dynamic_pointer_cast<ValueSet<T>>(lhs);
    auto right = std::dynamic_pointer_cast<ValueSet<T>>(rhs);

    ValueSet<T> result;
    std::set_union(
        left->values.begin(),
        left->values.end(),
        right->values.begin(),
        right->values.end(),
        std::inserter(result.values, result.values.begin()));

    return std::make_shared<ValueSet<T>>(result);
}

template <typename T>
IntegerSetPtr ValueSet<T>::unionWithRangeSet(const IntegerSetPtr & range_set, const IntegerSetPtr & value_set)
{
    RUNTIME_CHECK(range_set->getType() == SetType::Range);
    RUNTIME_CHECK(value_set->getType() == SetType::Value);

    auto range = std::dynamic_pointer_cast<RangeSet<T>>(range_set);
    auto value = std::dynamic_pointer_cast<ValueSet<T>>(value_set);

    ValueSet<T> exclude_value;
    for (const auto v : value->values)
    {
        if (range->start > v || v > range->end)
            exclude_value.values.insert(v);
    }

    return exclude_value.values.empty()
        ? range_set
        : std::make_shared<CompositeSet<T>>(
            std::vector<IntegerSetPtr>{std::make_shared<ValueSet<T>>(exclude_value), range_set});
}

template <typename T>
IntegerSetPtr ValueSet<T>::unionWithCompositeSet(const IntegerSetPtr & composite_set, const IntegerSetPtr & value_set)
{
    RUNTIME_CHECK(composite_set->getType() == SetType::Composite);
    RUNTIME_CHECK(value_set->getType() == SetType::Value);

    auto composite = std::dynamic_pointer_cast<CompositeSet<T>>(composite_set);
    auto value = std::dynamic_pointer_cast<ValueSet<T>>(value_set);

    CompositeSet<T> result;
    for (const auto & set : composite->sets)
    {
        auto new_set = set->unionWith(value);
        if (new_set && new_set->getType() == SetType::All)
            return AllSet::instance();
        else if (new_set)
            result.sets.push_back(new_set);
    }

    return std::make_shared<CompositeSet<T>>(result);
}

template <typename T>
IntegerSetPtr ValueSet<T>::invert() const
{
    std::vector<IntegerSetPtr> sets;
    T min = std::numeric_limits<T>::min();
    for (const auto & value : values)
    {
        if (value > min)
            sets.push_back(std::make_shared<RangeSet<T>>(min, value - 1));
        min = value + 1;
    }
    if (min <= std::numeric_limits<T>::max())
        sets.push_back(std::make_shared<RangeSet<T>>(min, std::numeric_limits<T>::max()));
    return std::make_shared<CompositeSet<T>>(sets);
}

template <typename T>
BitmapFilterPtr ValueSet<T>::search(InvertedIndexReaderPtr inverted_index, size_t size)
{
    auto filter = std::make_shared<BitmapFilter>(size, false);
    for (const auto & value : values)
        inverted_index->search(filter, value);
    return filter;
}

template <typename T>
IntegerSetPtr RangeSet<T>::intersectWith(const IntegerSetPtr & other)
{
    auto type = other->getType();
    switch (type)
    {
    case SetType::Value:
        return ValueSet<T>::intersectWithRangeSet(other, this->shared_from_this());
    case SetType::Range:
        return RangeSet<T>::intersectWithRangeSet(this->shared_from_this(), other);
    case SetType::Composite:
        return RangeSet<T>::intersectWithCompositeSet(other, this->shared_from_this());
    case SetType::All:
    case SetType::Empty:
        return other->intersectWith(this->shared_from_this());
    }
}

template <typename T>
IntegerSetPtr RangeSet<T>::intersectWithRangeSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs)
{
    RUNTIME_CHECK(lhs->getType() == SetType::Range);
    RUNTIME_CHECK(rhs->getType() == SetType::Range);

    auto left = std::dynamic_pointer_cast<RangeSet<T>>(lhs);
    auto right = std::dynamic_pointer_cast<RangeSet<T>>(rhs);

    T start = std::max(left->start, right->start);
    T end = std::min(left->end, right->end);

    return start > end ? EmptySet::instance() : std::make_shared<RangeSet<T>>(start, end);
}

template <typename T>
IntegerSetPtr RangeSet<T>::intersectWithCompositeSet(
    const IntegerSetPtr & composite_set,
    const IntegerSetPtr & range_set)
{
    RUNTIME_CHECK(composite_set->getType() == SetType::Composite);
    RUNTIME_CHECK(range_set->getType() == SetType::Range);

    auto composite = std::dynamic_pointer_cast<CompositeSet<T>>(composite_set);
    auto range = std::dynamic_pointer_cast<RangeSet<T>>(range_set);

    CompositeSet<T> result;
    for (const auto & set : composite->sets)
    {
        if (auto new_set = set->intersectWith(range); new_set)
            result.sets.push_back(new_set);
    }

    return result.sets.empty() ? EmptySet::instance() : std::make_shared<CompositeSet<T>>(result);
}

template <typename T>
IntegerSetPtr RangeSet<T>::unionWith(const IntegerSetPtr & other)
{
    auto type = other->getType();
    switch (type)
    {
    case SetType::Value:
        return ValueSet<T>::unionWithRangeSet(other, this->shared_from_this());
    case SetType::Range:
        return RangeSet<T>::unionWithRangeSet(this->shared_from_this(), other);
    case SetType::Composite:
        return RangeSet<T>::unionWithCompositeSet(other, this->shared_from_this());
    case SetType::All:
    case SetType::Empty:
        return other->unionWith(this->shared_from_this());
    }
}

template <typename T>
IntegerSetPtr RangeSet<T>::unionWithRangeSet(const IntegerSetPtr & lhs, const IntegerSetPtr & rhs)
{
    RUNTIME_CHECK(lhs->getType() == SetType::Range);
    RUNTIME_CHECK(rhs->getType() == SetType::Range);

    auto left = std::dynamic_pointer_cast<RangeSet<T>>(lhs);
    auto right = std::dynamic_pointer_cast<RangeSet<T>>(rhs);

    if (left->start > right->start)
        std::swap(left, right);

    T start = left->start;
    T end = std::max(left->end, right->end);

    if (left->end >= right->start)
        return start == std::numeric_limits<T>::min() && end == std::numeric_limits<T>::max()
            ? AllSet::instance()
            : std::make_shared<RangeSet<T>>(start, end);

    return std::make_shared<CompositeSet<T>>(std::vector<IntegerSetPtr>{lhs, rhs});
}

template <typename T>
IntegerSetPtr RangeSet<T>::unionWithCompositeSet(const IntegerSetPtr & composite_set, const IntegerSetPtr & range_set)
{
    RUNTIME_CHECK(composite_set->getType() == SetType::Composite);
    RUNTIME_CHECK(range_set->getType() == SetType::Range);

    auto composite = std::dynamic_pointer_cast<CompositeSet<T>>(composite_set);
    auto range = std::dynamic_pointer_cast<RangeSet<T>>(range_set);

    CompositeSet<T> result;
    for (const auto & set : composite->sets)
    {
        auto new_set = set->unionWith(range);
        if (new_set && new_set->getType() == SetType::All)
            return AllSet::instance();
        else if (new_set)
            result.sets.push_back(new_set);
    }

    return std::make_shared<CompositeSet<T>>(result);
}

template <typename T>
IntegerSetPtr RangeSet<T>::invert() const
{
    if (start == std::numeric_limits<T>::min() && end == std::numeric_limits<T>::max())
        return EmptySet::instance();
    if (start == std::numeric_limits<T>::min())
        return std::make_shared<RangeSet<T>>(end + 1, std::numeric_limits<T>::max());
    if (end == std::numeric_limits<T>::max())
        return std::make_shared<RangeSet<T>>(std::numeric_limits<T>::min(), start - 1);
    auto left = std::make_shared<RangeSet<T>>(std::numeric_limits<T>::min(), start - 1);
    auto right = std::make_shared<RangeSet<T>>(end + 1, std::numeric_limits<T>::max());
    return std::make_shared<CompositeSet<T>>(std::vector<IntegerSetPtr>{left, right});
}

template <typename T>
BitmapFilterPtr RangeSet<T>::search(InvertedIndexReaderPtr inverted_index, size_t size)
{
    auto filter = std::make_shared<BitmapFilter>(size, false);
    inverted_index->searchRange(filter, start, end);
    return filter;
}

template <typename T>
IntegerSetPtr CompositeSet<T>::intersectWith(const IntegerSetPtr & other)
{
    auto type = other->getType();
    switch (type)
    {
    case SetType::Value:
        return ValueSet<T>::intersectWithCompositeSet(this->shared_from_this(), other);
    case SetType::Range:
        return RangeSet<T>::intersectWithCompositeSet(this->shared_from_this(), other);
    case SetType::Composite:
    {
        CompositeSet<T> result;
        for (const auto & set : sets)
        {
            if (auto new_set = set->intersectWith(other); new_set)
                result.sets.push_back(new_set);
        }
        return result.sets.empty() ? EmptySet::instance() : std::make_shared<CompositeSet<T>>(result);
    }
    case SetType::All:
    case SetType::Empty:
        return other->intersectWith(this->shared_from_this());
    }
}

template <typename T>
IntegerSetPtr CompositeSet<T>::unionWith(const IntegerSetPtr & other)
{
    auto type = other->getType();
    switch (type)
    {
    case SetType::Value:
        return ValueSet<T>::unionWithCompositeSet(other, this->shared_from_this());
    case SetType::Range:
        return RangeSet<T>::unionWithCompositeSet(other, this->shared_from_this());
    case SetType::Composite:
    {
        CompositeSet<T> result;
        for (const auto & set : sets)
        {
            auto new_set = set->unionWith(other);
            if (new_set && new_set->getType() == SetType::All)
                return AllSet::instance();
            else if (new_set)
                result.sets.push_back(new_set);
        }
        return std::make_shared<CompositeSet<T>>(result);
    }
    case SetType::All:
    case SetType::Empty:
        return other->unionWith(this->shared_from_this());
    }
}

template <typename T>
IntegerSetPtr CompositeSet<T>::invert() const
{
    CompositeSet<T> result;
    for (const auto & set : sets)
    {
        if (auto new_set = set->invert(); new_set)
            result.sets.push_back(new_set);
    }

    // Combine all sets
    IntegerSetPtr result_set = result.sets.empty() ? EmptySet::instance() : result.sets.front();
    for (const auto & set : result.sets)
    {
        if (!result_set)
            break;

        result_set = result_set->intersectWith(set);
    }
    return result_set;
}

template <typename T>
BitmapFilterPtr CompositeSet<T>::search(InvertedIndexReaderPtr inverted_index, size_t size)
{
    BitmapFilterPtr filter = std::make_shared<BitmapFilter>(size, false);
    for (const auto & set : sets)
    {
        switch (set->getType())
        {
        case SetType::All:
            return std::make_shared<BitmapFilter>(size, true);
        case SetType::Empty:
            break;
        case SetType::Value:
        case SetType::Range:
        case SetType::Composite:
            auto sub_filter = set->search(inverted_index, size);
            filter->logicalOr(*sub_filter);
            break;
        }
    }
    return filter;
}

template class RangeSet<UInt8>;
template class RangeSet<UInt16>;
template class RangeSet<UInt32>;
template class RangeSet<UInt64>;
template class RangeSet<Int8>;
template class RangeSet<Int16>;
template class RangeSet<Int32>;
template class RangeSet<Int64>;
template class ValueSet<UInt8>;
template class ValueSet<UInt16>;
template class ValueSet<UInt32>;
template class ValueSet<UInt64>;
template class ValueSet<Int8>;
template class ValueSet<Int16>;
template class ValueSet<Int32>;
template class ValueSet<Int64>;
template class CompositeSet<UInt8>;
template class CompositeSet<UInt16>;
template class CompositeSet<UInt32>;
template class CompositeSet<UInt64>;
template class CompositeSet<Int8>;
template class CompositeSet<Int16>;
template class CompositeSet<Int32>;
template class CompositeSet<Int64>;

} // namespace DB::DM
