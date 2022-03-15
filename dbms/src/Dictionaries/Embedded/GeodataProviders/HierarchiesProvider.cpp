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

#include <Dictionaries/Embedded/GeodataProviders/HierarchiesProvider.h>
#include <Dictionaries/Embedded/GeodataProviders/HierarchyFormatReader.h>

#include <IO/ReadBufferFromFile.h>

#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/DirectoryIterator.h>


bool RegionsHierarchyDataSource::isModified() const
{
    return updates_tracker.isModified();
}

IRegionsHierarchyReaderPtr RegionsHierarchyDataSource::createReader()
{
    updates_tracker.fixCurrentVersion();
    auto file_reader = std::make_shared<DB::ReadBufferFromFile>(path);
    return std::make_unique<RegionsHierarchyFormatReader>(std::move(file_reader));
}


RegionsHierarchiesDataProvider::RegionsHierarchiesDataProvider(const std::string & path)
    : path(path)
{
    discoverFilesWithCustomHierarchies();
}

void RegionsHierarchiesDataProvider::discoverFilesWithCustomHierarchies()
{
    std::string basename = Poco::Path(path).getBaseName();

    Poco::Path dir_path = Poco::Path(path).absolute().parent();

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(dir_path); dir_it != dir_end; ++dir_it)
    {
        std::string candidate_basename = dir_it.path().getBaseName();

        if ((0 == candidate_basename.compare(0, basename.size(), basename)) &&
            (candidate_basename.size() > basename.size() + 1) &&
            (candidate_basename[basename.size()] == '_'))
        {
            const std::string suffix = candidate_basename.substr(basename.size() + 1);
            hierarchy_files.emplace(suffix, dir_it->path());
        }
    }
}

std::vector<std::string> RegionsHierarchiesDataProvider::listCustomHierarchies() const
{
    std::vector<std::string> names;
    names.reserve(hierarchy_files.size());
    for (const auto & it : hierarchy_files)
        names.push_back(it.first);
    return names;
}

IRegionsHierarchyDataSourcePtr RegionsHierarchiesDataProvider::getDefaultHierarchySource() const
{
    return std::make_shared<RegionsHierarchyDataSource>(path);
}

IRegionsHierarchyDataSourcePtr RegionsHierarchiesDataProvider::getHierarchySource(const std::string & name) const
{
    auto found = hierarchy_files.find(name);

    if (found != hierarchy_files.end())
    {
        const auto & hierarchy_file_path = found->second;
        return std::make_shared<RegionsHierarchyDataSource>(hierarchy_file_path);
    }

    throw Poco::Exception("Regions hierarchy `" + name + "` not found");
}
