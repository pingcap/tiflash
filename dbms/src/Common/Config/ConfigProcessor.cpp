/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include <Common/Config/cpptoml.h>

#if !__clang__
#pragma GCC diagnostic pop
#endif

#include <Common/Config/TOMLConfiguration.h>
#include <Common/StringUtils/StringUtils.h>
#include <sys/utsname.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <functional>
#include <iostream>

#include "ConfigProcessor.h"

#define PREPROCESSED_SUFFIX "-preprocessed"

static std::string preprocessedConfigPath(const std::string & path)
{
    Poco::Path preprocessed_path(path);
    preprocessed_path.setBaseName(preprocessed_path.getBaseName() + PREPROCESSED_SUFFIX);
    return preprocessed_path.toString();
}

bool ConfigProcessor::isPreprocessedFile(const std::string & path) { return endsWith(Poco::Path(path).getBaseName(), PREPROCESSED_SUFFIX); }


ConfigProcessor::ConfigProcessor(const std::string & path_, bool log_to_console, const Substitutions & substitutions_)
    : path(path_), preprocessed_path(preprocessedConfigPath(path)), substitutions(substitutions_)
{
    if (log_to_console && Logger::has("ConfigProcessor") == nullptr)
    {
        channel_ptr = new Poco::ConsoleChannel;
        log = &Logger::create("ConfigProcessor", channel_ptr.get(), Poco::Message::PRIO_TRACE);
    }
    else
    {
        log = &Logger::get("ConfigProcessor");
    }
}

ConfigProcessor::~ConfigProcessor()
{
    if (channel_ptr) /// This means we have created a new console logger in the constructor.
        Logger::destroy("ConfigProcessor");
}

TOMLTablePtr ConfigProcessor::processConfig() { return cpptoml::parse_file(path); }

ConfigProcessor::LoadedConfig ConfigProcessor::loadConfig()
{
    TOMLTablePtr config_doc = processConfig();

    ConfigurationPtr configuration(new DB::TOMLConfiguration(config_doc));

    return LoadedConfig{configuration, false, config_doc};
}

void ConfigProcessor::savePreprocessedConfig(const LoadedConfig & loaded_config)
{
    std::ofstream out(preprocessed_path);
    cpptoml::toml_writer writer(out);
    loaded_config.preprocessed_conf->accept(std::move(writer));
    out.close();
}
