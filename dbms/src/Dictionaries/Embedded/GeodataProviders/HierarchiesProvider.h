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

#include <Dictionaries/Embedded/GeodataProviders/IHierarchiesProvider.h>

#include <Common/FileUpdatesTracker.h>

#include <unordered_map>


// Represents local file with regions hierarchy dump
class RegionsHierarchyDataSource
    : public IRegionsHierarchyDataSource
{
private:
    std::string path;
    FileUpdatesTracker updates_tracker;

public:
    RegionsHierarchyDataSource(const std::string & path_)
        : path(path_)
        , updates_tracker(path_)
    {}

    bool isModified() const override;

    IRegionsHierarchyReaderPtr createReader() override;
};


// Provides access to directory with multiple data source files: one file per regions hierarchy
class RegionsHierarchiesDataProvider
    : public IRegionsHierarchiesDataProvider
{
private:
    // path to file with default regions hierarchy
    std::string path;

    using HierarchyFiles = std::unordered_map<std::string, std::string>;
    HierarchyFiles hierarchy_files;

public:
    /** path must point to the file with the hierarchy of regions "by default". It will be accessible by an empty key.
      * In addition, a number of files are searched for, the name of which (before the extension, if any) is added arbitrary _suffix.
      * Such files are loaded, and the hierarchy of regions is put on the `suffix` key.
      *
      * For example, if /opt/geo/regions_hierarchy.txt is specified,
      *  then the /opt/geo/regions_hierarchy_ua.txt file will also be loaded, if any, it will be accessible by the `ua` key.
      */
    RegionsHierarchiesDataProvider(const std::string & path);

    std::vector<std::string> listCustomHierarchies() const override;

    IRegionsHierarchyDataSourcePtr getDefaultHierarchySource() const override;
    IRegionsHierarchyDataSourcePtr getHierarchySource(const std::string & name) const override;

private:
    void discoverFilesWithCustomHierarchies();
};

