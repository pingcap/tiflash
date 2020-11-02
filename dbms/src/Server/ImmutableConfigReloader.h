#pragma once

#include <Common/Config/ConfigReloader.h>
#include <Common/Config/TOMLConfiguration.h>

namespace DB
{
class ImmutableConfigReloader : public ConfigReloader
{
public:
    ImmutableConfigReloader(ConfigReloader::Updater && updater) : ConfigReloader("", std::move(updater), true)
    {
        reloadIfNewer(/* force = */ true, /* throw_on_error = */ true);
    }
    ~ImmutableConfigReloader() {}
    void start() override {}
    void reloadIfNewer(bool, bool throw_on_error) override
    {
        auto configuration = genDefaultConfig();

        std::lock_guard<std::mutex> lock(reload_mutex);

        try
        {
            getUpdater()(configuration);
        }
        catch (...)
        {
            if (throw_on_error)
                throw;
            tryLogCurrentException(log, "Error updating configuration from immutable config.");
        }
    }

private:
    ConfigurationPtr genDefaultConfig()
    {
        const char * DefaultConfig = "[quotas]\n"
                                     "[quotas.default]\n"
                                     "[quotas.default.interval]\n"
                                     "result_rows = 0\n"
                                     "read_rows = 0\n"
                                     "execution_time = 0\n"
                                     "queries = 0\n"
                                     "errors = 0\n"
                                     "duration = 3600\n"
                                     "[users]\n"
                                     "[users.readonly]\n"
                                     "quota = \"default\"\n"
                                     "profile = \"readonly\"\n"
                                     "password = \"\"\n"
                                     "[users.readonly.networks]\n"
                                     "ip = \"::/0\"\n"
                                     "[users.default]\n"
                                     "quota = \"default\"\n"
                                     "profile = \"default\"\n"
                                     "password = \"\"\n"
                                     "[users.default.networks]\n"
                                     "ip = \"::/0\"\n"
                                     "[profiles]\n"
                                     "[profiles.readonly]\n"
                                     "readonly = 1\n"
                                     "[profiles.default]\n"
                                     "load_balancing = \"random\"\n"
                                     "use_uncompressed_cache = 0\n"
                                     "max_memory_usage = 10000000000\n";
        std::istringstream iss(DefaultConfig);
        cpptoml::parser p(iss);
        ConfigurationPtr configuration(new DB::TOMLConfiguration(p.parse()));
        return configuration;
    }

private:
    std::mutex reload_mutex;
    Poco::Logger * log = &Logger::get("ImmutableConfigReloader");
};
} // namespace DB
