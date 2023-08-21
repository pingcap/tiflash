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

/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif

#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/TOMLConfiguration.h>
#include <Common/StringUtils/StringUtils.h>
#include <string.h>
#include <sys/utsname.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <functional>
#include <iostream>

#define PREPROCESSED_SUFFIX "-preprocessed"

static std::string preprocessedConfigPath(const std::string & path)
{
    Poco::Path preprocessed_path(path);
    preprocessed_path.setBaseName(preprocessed_path.getBaseName() + PREPROCESSED_SUFFIX);
    return preprocessed_path.toString();
}

bool ConfigProcessor::isPreprocessedFile(const std::string & path)
{
    return endsWith(Poco::Path(path).getBaseName(), PREPROCESSED_SUFFIX);
}


ConfigProcessor::ConfigProcessor(const std::string & path_, bool log_to_console, const Substitutions & substitutions_)
    : path(path_)
    , preprocessed_path(preprocessedConfigPath(path))
    , substitutions(substitutions_)
{
    if (log_to_console && Poco::Logger::has("ConfigProcessor") == nullptr)
    {
        channel_ptr = new Poco::ConsoleChannel;
        log = &Poco::Logger::create("ConfigProcessor", channel_ptr.get(), Poco::Message::PRIO_TRACE);
    }
    else
    {
        log = &Poco::Logger::get("ConfigProcessor");
    }
}

ConfigProcessor::~ConfigProcessor()
{
    if (channel_ptr) /// This means we have created a new console logger in the constructor.
        Poco::Logger::destroy("ConfigProcessor");
}

TOMLTablePtr ConfigProcessor::processConfig()
{
    return cpptoml::parse_file(path);
}

ConfigProcessor::LoadedConfig ConfigProcessor::loadConfig()
{
    TOMLTablePtr config_doc = processConfig();

    ConfigurationPtr configuration(new DB::TOMLConfiguration(config_doc));

    return LoadedConfig{configuration, false, config_doc};
}
