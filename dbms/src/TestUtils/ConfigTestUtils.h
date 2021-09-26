#pragma once

#include <Common/Config/TOMLConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <cpptoml.h>
namespace DB
{
namespace tests
{
inline static auto loadConfigFromString(const String & s)
{
    std::istringstream ss(s);
    cpptoml::parser p(ss);
    auto table = p.parse();
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
    config->add(new DB::TOMLConfiguration(table), false); // Take ownership of TOMLConfig
    return config;
}
} // namespace tests
} // namespace DB
