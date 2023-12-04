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

#include <Dictionaries/Embedded/RegionsHierarchies.h>
#include <Poco/DirectoryIterator.h>
#include <common/logger_useful.h>


RegionsHierarchies::RegionsHierarchies(IRegionsHierarchiesDataProviderPtr data_provider)
{
    Poco::Logger * log = &Poco::Logger::get("RegionsHierarchies");

    LOG_DEBUG(log, "Adding default regions hierarchy");
    data.emplace("", data_provider->getDefaultHierarchySource());

    for (const auto & name : data_provider->listCustomHierarchies())
    {
        LOG_FMT_DEBUG(log, "Adding regions hierarchy for {}", name);
        data.emplace(name, data_provider->getHierarchySource(name));
    }

    reload();
}
