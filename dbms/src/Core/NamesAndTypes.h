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

#pragma once

#include <Core/Names.h>
#include <DataTypes/IDataType.h>

#include <initializer_list>
#include <list>
#include <map>
#include <set>
#include <string>


namespace DB
{
struct NameAndTypePair
{
    String name;
    DataTypePtr type;

    NameAndTypePair() = default;
    NameAndTypePair(const String & name_, const DataTypePtr & type_)
        : name(name_)
        , type(type_)
    {}

    bool operator<(const NameAndTypePair & rhs) const
    {
        return std::forward_as_tuple(name, type->getName()) < std::forward_as_tuple(rhs.name, rhs.type->getName());
    }

    bool operator==(const NameAndTypePair & rhs) const { return name == rhs.name && type->equals(*rhs.type); }
};

using NamesAndTypes = std::vector<NameAndTypePair>;

String dumpJsonStructure(const NamesAndTypes & names_and_types);

Names toNames(const NamesAndTypes & names_and_types);

class NamesAndTypesList : public std::list<NameAndTypePair>
{
public:
    using Iterator = std::list<NameAndTypePair>::iterator;

    NamesAndTypesList() = default;

    NamesAndTypesList(std::initializer_list<NameAndTypePair> init)
        : std::list<NameAndTypePair>(init)
    {}

    template <typename Iterator>
    NamesAndTypesList(Iterator begin, Iterator end)
        : std::list<NameAndTypePair>(begin, end)
    {}

    void readText(ReadBuffer & buf);
    void writeText(WriteBuffer & buf) const;

    String toString() const;
    static NamesAndTypesList parse(const String & s);

    /// All `rhs` elements must be different.
    bool isSubsetOf(const NamesAndTypesList & rhs) const;

    /// Hamming distance between sets
    ///  (in other words, the added and deleted columns are counted once, the columns that changed the type - twice).
    size_t sizeOfDifference(const NamesAndTypesList & rhs) const;

    Names getNames() const;
    DataTypes getTypes() const;

    /// Leave only the columns whose names are in the `names`. In `names` there can be superfluous columns.
    NamesAndTypesList filter(const NameSet & names) const;

    /// Leave only the columns whose names are in the `names`. In `names` there can be superfluous columns.
    NamesAndTypesList filter(const Names & names) const;

    /// Unlike `filter`, returns columns in the order in which they go in `names`.
    NamesAndTypesList addTypes(const Names & names) const;

    bool contains(const String & name) const;
};

} // namespace DB
