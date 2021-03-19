#include <Common/Config/ConfigReloader.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/UserConfigParser.h>
#include <common/logger_useful.h>

#include <memory>

namespace DB
{
namespace UserConfig
{

static std::string tryGetAbsolutePath(const std::string & config_path, std::string && users_config_path)
{
    if (users_config_path[0] != '/')
    {
        /// If path to users' config isn't absolute, try guess its root (current) dir.
        /// Try to find it in dir of main config dir.
        /// If the config file is not exists, we will return an empty path.
        std::string config_parent_dir = Poco::Path(config_path).parent().toString();
        if (auto f = Poco::File(config_parent_dir + users_config_path); f.exists())
        {
            users_config_path = config_parent_dir + users_config_path;
        }
        else
        {
            users_config_path.clear();
        }
    }
    return std::move(users_config_path);
}

ConfigReloaderPtr parseSettings(Poco::Util::LayeredConfiguration & config, const std::string & config_path,
    std::unique_ptr<Context> & global_context, Poco::Logger * log)
{
    std::string users_config_path = config.getString("users_config", String(1, '\0'));
    bool load_from_main_config_path = true;
    if (users_config_path.empty() || users_config_path[0] == '\0')
        load_from_main_config_path = true;
    else
    {
        /// If path to users' config isn't absolute, try guess its root (current) dir.
        users_config_path = tryGetAbsolutePath(config_path, std::move(users_config_path));
        /// If the config file is not exists, we will use `config_path` as the user config file
        load_from_main_config_path = users_config_path.empty();
    }

    if (load_from_main_config_path)
        users_config_path = config_path;

    LOG_INFO(log, "Set users config file to: " << users_config_path);

    return std::make_unique<ConfigReloader>(
        users_config_path, //
        /*updater=*/[&global_context](ConfigurationPtr cfg) { global_context->setUsersConfig(cfg); },
        /*already_loaded=*/false, /*name=*/"UserCfgReloader");
}

} // namespace UserConfig
} // namespace DB
