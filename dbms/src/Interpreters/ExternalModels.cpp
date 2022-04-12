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

#include <Interpreters/Context.h>
#include <Interpreters/ExternalModels.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
const ExternalLoaderUpdateSettings externalModelsUpdateSettings{};

const ExternalLoaderConfigSettings & getExternalModelsConfigSettings()
{
    static ExternalLoaderConfigSettings settings;
    static std::once_flag flag;

    std::call_once(flag, [] {
        settings.external_config = "model";
        settings.external_name = "name";
        settings.path_setting_name = "models_config";
    });

    return settings;
}
} // namespace


ExternalModels::ExternalModels(
    std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
    Context & context,
    bool throw_on_error)
    : ExternalLoader(context.getConfigRef(),
                     externalModelsUpdateSettings,
                     getExternalModelsConfigSettings(),
                     std::move(config_repository),
                     &Poco::Logger::get("ExternalModels"),
                     "external model")
    , context(context)
{
    init(throw_on_error);
}

std::unique_ptr<IExternalLoadable> ExternalModels::create(
    const std::string & name,
    const Configuration & config,
    const std::string & config_prefix)
{
    String type = config.getString(config_prefix + ".type");
    ExternalLoadableLifetime lifetime(config, config_prefix + ".lifetime");

    /// TODO: add models factory.
    if (type == "catboost")
    {
        return std::make_unique<CatBoostModel>(
            name,
            config.getString(config_prefix + ".path"),
            context.getConfigRef().getString("catboost_dynamic_library_path"),
            lifetime);
    }
    else
    {
        throw Exception("Unknown model type: " + type, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
}

} // namespace DB
