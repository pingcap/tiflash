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

#include <Dictionaries/Embedded/RegionsHierarchy.h>
#include <Dictionaries/Embedded/GeodataProviders/IHierarchiesProvider.h>

#include <Poco/Exception.h>

#include <unordered_map>


/** Contains several hierarchies of regions.
  * Used to support several different perspectives on the ownership of regions by countries.
  * First of all, for the Crimea (Russian and Ukrainian points of view).
  */
class RegionsHierarchies
{
private:
    using Container = std::unordered_map<std::string, RegionsHierarchy>;
    Container data;

public:
    RegionsHierarchies(IRegionsHierarchiesDataProviderPtr data_provider);

    /** Reloads, if necessary, all hierarchies of regions.
      */
    void reload()
    {
        for (auto & elem : data)
            elem.second.reload();
    }


    const RegionsHierarchy & get(const std::string & key) const
    {
        auto it = data.find(key);

        if (data.end() == it)
            throw Poco::Exception("There is no regions hierarchy for key " + key);

        return it->second;
    }
};
