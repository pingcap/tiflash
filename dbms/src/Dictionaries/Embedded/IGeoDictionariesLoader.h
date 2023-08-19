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

#include <Dictionaries/Embedded/RegionsHierarchies.h>
#include <Dictionaries/Embedded/RegionsNames.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <memory>


// Provides actual versions of geo dictionaries (regions hierarchies, regions names)
// Bind data structures (RegionsHierarchies, RegionsNames) with data providers
class IGeoDictionariesLoader
{
public:
    virtual std::unique_ptr<RegionsHierarchies> reloadRegionsHierarchies(
        const Poco::Util::AbstractConfiguration & config) = 0;

    virtual std::unique_ptr<RegionsNames> reloadRegionsNames(
        const Poco::Util::AbstractConfiguration & config) = 0;

    virtual ~IGeoDictionariesLoader() {}
};
