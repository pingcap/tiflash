// Copyright 2022 PingCAP, Ltd.
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

#include <Dictionaries/Embedded/GeodataProviders/Entries.h>

#include <memory>
#include <string>
#include <vector>


// Iterates over all regions in data source
class IRegionsHierarchyReader
{
public:
    virtual bool readNext(RegionEntry & entry) = 0;

    virtual ~IRegionsHierarchyReader() {}
};

using IRegionsHierarchyReaderPtr = std::unique_ptr<IRegionsHierarchyReader>;


// Data source for single regions hierarchy
class IRegionsHierarchyDataSource
{
public:
    // data modified since last createReader invocation
    virtual bool isModified() const = 0;

    virtual IRegionsHierarchyReaderPtr createReader() = 0;

    virtual ~IRegionsHierarchyDataSource() {}
};

using IRegionsHierarchyDataSourcePtr = std::shared_ptr<IRegionsHierarchyDataSource>;


// Provides data sources for different regions hierarchies
class IRegionsHierarchiesDataProvider
{
public:
    virtual std::vector<std::string> listCustomHierarchies() const = 0;

    virtual IRegionsHierarchyDataSourcePtr getDefaultHierarchySource() const = 0;
    virtual IRegionsHierarchyDataSourcePtr getHierarchySource(const std::string & name) const = 0;

    virtual ~IRegionsHierarchiesDataProvider() {};
};

using IRegionsHierarchiesDataProviderPtr = std::shared_ptr<IRegionsHierarchiesDataProvider>;

