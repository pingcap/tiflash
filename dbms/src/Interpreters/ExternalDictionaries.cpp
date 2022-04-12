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

#include <Dictionaries/DictionaryFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>

namespace DB
{
namespace
{
const ExternalLoaderUpdateSettings externalDictionariesUpdateSettings{};

const ExternalLoaderConfigSettings & getExternalDictionariesConfigSettings()
{
    static ExternalLoaderConfigSettings settings;
    static std::once_flag flag;

    std::call_once(flag, [] {
        settings.external_config = "dictionary";
        settings.external_name = "name";
        settings.path_setting_name = "dictionaries_config";
    });

    return settings;
}
} // namespace


ExternalDictionaries::ExternalDictionaries(
    std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
    Context & context,
    bool throw_on_error)
    : ExternalLoader(context.getConfigRef(),
                     externalDictionariesUpdateSettings,
                     getExternalDictionariesConfigSettings(),
                     std::move(config_repository),
                     &Poco::Logger::get("ExternalDictionaries"),
                     "external dictionary")
    , context(context)
{
    init(throw_on_error);
}

std::unique_ptr<IExternalLoadable> ExternalDictionaries::create(
    const std::string & name,
    const Configuration & config,
    const std::string & config_prefix)
{
    return DictionaryFactory::instance().create(name, config, config_prefix, context);
}

} // namespace DB
