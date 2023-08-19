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

#include <Dictionaries/IDictionary.h>
#include <Interpreters/ExternalLoader.h>
#include <common/logger_useful.h>

#include <memory>


namespace DB
{
class Context;

/// Manages user-defined dictionaries.
class ExternalDictionaries : public ExternalLoader
{
public:
    using DictPtr = std::shared_ptr<IDictionaryBase>;

    /// Dictionaries will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    ExternalDictionaries(
        std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
        Context & context,
        bool throw_on_error);

    /// Forcibly reloads specified dictionary.
    void reloadDictionary(const std::string & name) { reload(name); }

    DictPtr getDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<IDictionaryBase>(getLoadable(name));
    }

    DictPtr tryGetDictionary(const std::string & name) const
    {
        return std::static_pointer_cast<IDictionaryBase>(tryGetLoadable(name));
    }

protected:
    std::unique_ptr<IExternalLoadable> create(const std::string & name, const Configuration & config, const std::string & config_prefix) override;

    using ExternalLoader::getObjectsMap;

    friend class StorageSystemDictionaries;
    friend class DatabaseDictionary;

private:
    Context & context;
};

} // namespace DB
