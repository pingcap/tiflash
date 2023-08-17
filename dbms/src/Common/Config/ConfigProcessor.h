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

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>

#include <string>
#include <unordered_set>
#include <vector>


using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
namespace cpptoml
{
class table;
}

using TOMLTablePtr = std::shared_ptr<cpptoml::table>;

class ConfigProcessor
{
public:
    using Substitutions = std::vector<std::pair<std::string, std::string>>;

    /// Set log_to_console to true if the logging subsystem is not initialized yet.
    explicit ConfigProcessor(
        const std::string & path,
        bool log_to_console = false,
        const Substitutions & substitutions = Substitutions());

    ~ConfigProcessor();

    TOMLTablePtr processConfig();


    /// loadConfig* functions apply processConfig and create Poco::Util::XMLConfiguration.
    /// The resulting XML document is saved into a file with the name
    /// resulting from adding "-preprocessed" suffix to the path file name.
    /// E.g., config.xml -> config-preprocessed.xml

    struct LoadedConfig
    {
        ConfigurationPtr configuration;
        bool loaded_from_preprocessed;
        TOMLTablePtr preprocessed_conf;
    };

    LoadedConfig loadConfig();

public:
    /// Is the file named as result of config preprocessing, not as original files.
    static bool isPreprocessedFile(const std::string & config_path);

private:
    const std::string path;
    const std::string preprocessed_path;

    Poco::Logger * log;
    Poco::AutoPtr<Poco::Channel> channel_ptr;

    Substitutions substitutions;
};
