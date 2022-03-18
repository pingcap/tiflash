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

#include <Dictionaries/Embedded/GeoDictionariesLoader.h>

#include <Dictionaries/Embedded/GeodataProviders/HierarchiesProvider.h>
#include <Dictionaries/Embedded/GeodataProviders/NamesProvider.h>


std::unique_ptr<RegionsHierarchies> GeoDictionariesLoader::reloadRegionsHierarchies(
    const Poco::Util::AbstractConfiguration & config)
{
    static constexpr auto config_key = "path_to_regions_hierarchy_file";

    if (!config.has(config_key))
        return {};

    const auto default_hierarchy_file = config.getString(config_key);
    auto data_provider = std::make_unique<RegionsHierarchiesDataProvider>(default_hierarchy_file); 
    return std::make_unique<RegionsHierarchies>(std::move(data_provider));
}

std::unique_ptr<RegionsNames> GeoDictionariesLoader::reloadRegionsNames(
    const Poco::Util::AbstractConfiguration & config)
{
    static constexpr auto config_key = "path_to_regions_names_files";

    if (!config.has(config_key))
        return {};

    const auto directory = config.getString(config_key);
    auto data_provider = std::make_unique<RegionsNamesDataProvider>(directory);
    return std::make_unique<RegionsNames>(std::move(data_provider));
}
