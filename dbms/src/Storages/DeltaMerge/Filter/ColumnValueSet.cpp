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

#include <Storages/DeltaMerge/Filter/ColumnValueSet.h>

namespace DB::DM
{

const ColumnValueSetPtr UnsupportedColumnValueSet::Instance = std::make_shared<UnsupportedColumnValueSet>();

ColumnValueSetPtr AndColumnValueSet::invert() const
{
    ColumnValueSets inverted_children;
    inverted_children.reserve(children.size());
    for (const auto & child : children)
        inverted_children.push_back(child->invert());
    return OrColumnValueSet::create(inverted_children);
}

ColumnValueSetPtr AndColumnValueSet::tryOptimize()
{
    // Flatten nested AndColumnValueSet
    ColumnValueSets new_children;
    for (auto & child : children)
    {
        if (auto * and_child = dynamic_cast<AndColumnValueSet *>(child.get()); and_child)
            new_children.insert(new_children.end(), and_child->children.begin(), and_child->children.end());
        else
            new_children.push_back(child);
    }
    children.swap(new_children);
    new_children.clear();

    // Merge single sets on the same index by intersecting their integer sets
    // Remove UnsupportedColumnValueSet
    std::unordered_map<IndexID, std::pair<ColumnID, IntegerSetPtr>> merged_sets;
    for (auto & child : children)
    {
        if (auto single_child = std::dynamic_pointer_cast<SingleColumnValueSet>(child); single_child)
        {
            if (auto it = merged_sets.find(single_child->index_id); it != merged_sets.end())
                it->second = {single_child->column_id, it->second.second->intersectWith(single_child->set)};
            else
                merged_sets[single_child->index_id] = {single_child->column_id, single_child->set};
        }
        else if (child->type == ColumnValueSetType::Unsupported)
        {
            // Skip
        }
        else
        {
            // Non-single sets are kept as is
            new_children.push_back(child);
        }
    }

    // Replace children with merged single sets
    children.clear();
    for (const auto & [index_id, value] : merged_sets)
    {
        children.push_back(SingleColumnValueSet::create(value.first, index_id, value.second));
    }
    children.insert(children.end(), new_children.begin(), new_children.end());

    // If there is only one child, return that child
    if (children.empty())
        return UnsupportedColumnValueSet::Instance;
    if (children.size() == 1)
        return children.front();
    return this->shared_from_this();
}

BitmapFilterPtr AndColumnValueSet::check(
    std::function<BitmapFilterPtr(const ColumnValueSetPtr &, size_t size)> search,
    size_t size)
{
    BitmapFilterPtr result = nullptr;
    for (const auto & child : children)
    {
        auto child_result = child->check(search, size);
        if (!result || !child_result)
            result = child_result;
        else
            result->intersect(*child_result);
    }
    return result;
}

ColumnValueSetPtr OrColumnValueSet::invert() const
{
    ColumnValueSets inverted_children;
    inverted_children.reserve(children.size());
    for (const auto & child : children)
        inverted_children.push_back(child->invert());
    return AndColumnValueSet::create(inverted_children);
}

ColumnValueSetPtr OrColumnValueSet::tryOptimize()
{
    // Flatten nested OrColumnValueSet
    ColumnValueSets new_children;
    for (auto & child : children)
    {
        if (auto * or_child = dynamic_cast<OrColumnValueSet *>(child.get()); or_child)
            new_children.insert(new_children.end(), or_child->children.begin(), or_child->children.end());
        else
            new_children.push_back(child);
    }
    children.swap(new_children);
    new_children.clear();

    // Merge single sets on the same index by unioning their integer sets
    // Return UnsupportedColumnValueSet if there is any
    std::unordered_map<IndexID, std::pair<ColumnID, IntegerSetPtr>> merged_sets;
    for (auto & child : children)
    {
        if (auto single_child = std::dynamic_pointer_cast<SingleColumnValueSet>(child); single_child)
        {
            if (auto it = merged_sets.find(single_child->index_id); it != merged_sets.end())
                it->second = {single_child->column_id, it->second.second->unionWith(single_child->set)};
            else
                merged_sets[single_child->index_id] = {single_child->column_id, single_child->set};
        }
        else if (child->type == ColumnValueSetType::Unsupported)
        {
            return UnsupportedColumnValueSet::Instance;
        }
        else
        {
            // Non-single sets are kept as is
            new_children.push_back(child);
        }
    }

    // Replace children with merged single sets
    children.clear();
    for (const auto & [index_id, value] : merged_sets)
    {
        children.push_back(SingleColumnValueSet::create(value.first, index_id, value.second));
    }
    children.insert(children.end(), new_children.begin(), new_children.end());

    // If there is only one child, return that child
    if (children.size() == 1)
        return children.front();
    return this->shared_from_this();
}

BitmapFilterPtr OrColumnValueSet::check(
    std::function<BitmapFilterPtr(const ColumnValueSetPtr &, size_t size)> search,
    size_t size)
{
    BitmapFilterPtr result = nullptr;
    for (const auto & child : children)
    {
        auto child_result = child->check(search, size);
        if (!result || !child_result)
            result = child_result;
        else
            result->merge(*child_result);
    }
    return result;
}

} // namespace DB::DM
