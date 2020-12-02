#include "ClusterManagerService.h"

#include <Common/FunctionTimerTask.h>
#include <Common/ShellCommand.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <common/logger_useful.h>

namespace DB
{

const std::string TIFLASH_PREFIX = "flash";
const std::string CLUSTER_MANAGER_PATH_KEY = TIFLASH_PREFIX + ".flash_cluster.cluster_manager_path";
const std::string BIN_NAME = "flash_cluster_manager";
const std::string TASK_INTERVAL_KEY = TIFLASH_PREFIX + ".flash_cluster.update_rule_interval";

constexpr long MILLISECOND = 1000;
constexpr long INIT_DELAY = 5;

void ClusterManagerService::run(const std::string & bin_path, const std::vector<std::string> & args)
try
{
    auto proc = ShellCommand::executeDirect(bin_path, args);
    proc->wait();
}
catch (DB::Exception & e)
{
    std::stringstream ss;
    ss << bin_path;
    for (const auto & arg : args)
    {
        ss << " " << arg;
    }
    e.addMessage("(while running `" + ss.str() + "`)");
}

ClusterManagerService::ClusterManagerService(DB::Context & context_, const std::string & config_path)
    : context(context_), timer("ClusterManager"), log(&Logger::get("ClusterManagerService"))
{
    const auto & conf = context.getConfigRef();

    const auto default_bin_path = conf.getString("application.dir") + "flash_cluster_manager";

    if (!conf.has(TIFLASH_PREFIX))
    {
        LOG_WARNING(log, "TiFlash service is not specified, cluster manager can not be started");
        return;
    }

    if (!conf.has(CLUSTER_MANAGER_PATH_KEY))
    {
        LOG_WARNING(log, "Binary path of cluster manager is not set, try to use default: " << default_bin_path);
    }

    auto bin_path = conf.getString(CLUSTER_MANAGER_PATH_KEY, default_bin_path) + Poco::Path::separator() + BIN_NAME;
    auto task_interval = conf.getInt(TASK_INTERVAL_KEY, 10);

    if (!Poco::File(bin_path).exists())
    {
        LOG_ERROR(log, "Binary file of cluster manager does not exist in " << bin_path << ", can not sync tiflash replica");
        return;
    }

    std::vector<std::string> args;
    args.push_back("--config");
    args.push_back(config_path);

    LOG_INFO(log, "Registered timed cluster manager task at rate " << task_interval << " seconds");

    timer.scheduleAtFixedRate(FunctionTimerTask::create(std::bind(&ClusterManagerService::run, bin_path, args)), INIT_DELAY * MILLISECOND,
        task_interval * MILLISECOND);
}

ClusterManagerService::~ClusterManagerService() { timer.cancel(true); }

} // namespace DB
