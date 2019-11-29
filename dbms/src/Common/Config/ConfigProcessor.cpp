#include "ConfigProcessor.h"

#include <sys/utsname.h>
#include <cerrno>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <functional>
#include <Poco/DOM/Text.h>
#include <Poco/DOM/Attr.h>
#include <Poco/DOM/Comment.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Config/cpptoml.h>

#define PREPROCESSED_SUFFIX "-preprocessed"


using namespace Poco::XML;


/// Extracts from a string the first encountered number consisting of at least two digits.
static std::string numberFromHost(const std::string & s)
{
    for (size_t i = 0; i < s.size(); ++i)
    {
        std::string res;
        size_t j = i;
        while (j < s.size() && isNumericASCII(s[j]))
            res += s[j++];
        if (res.size() >= 2)
        {
            while (res[0] == '0')
                res.erase(res.begin());
            return res;
        }
    }
    return "";
}

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


ConfigProcessor::ConfigProcessor(
    const std::string & path_,
    bool throw_on_bad_incl_,
    bool log_to_console,
    const Substitutions & substitutions_)
    : path(path_)
    , preprocessed_path(preprocessedConfigPath(path))
    , throw_on_bad_incl(throw_on_bad_incl_)
    , substitutions(substitutions_)
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

static bool allWhitespace(const std::string & s)
{
    return s.find_first_not_of(" \t\n\r") == std::string::npos;
}

std::string ConfigProcessor::layerFromHost()
{
    utsname buf;
    if (uname(&buf))
        throw Poco::Exception(std::string("uname failed: ") + std::strerror(errno));

    std::string layer = numberFromHost(buf.nodename);
    if (layer.empty())
        throw Poco::Exception(std::string("no layer in host name: ") + buf.nodename);

    return layer;
}

TOMLTablePtr ConfigProcessor::processConfig()
{
    return cpptoml::parse_file(path);
}

ConfigProcessor::LoadedConfig ConfigProcessor::loadConfig()
{
    TOMLTablePtr config_xml = processConfig();

    ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    return LoadedConfig{configuration, /* loaded_from_preprocessed = */ false, config_xml};
}

void ConfigProcessor::savePreprocessedConfig(const LoadedConfig & loaded_config)
{
}
