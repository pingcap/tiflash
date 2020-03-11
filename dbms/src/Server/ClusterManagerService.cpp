#include "ClusterManagerService.h"

#include <Common/FunctionTimerTask.h>
#include <Common/ShellCommand.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <common/logger_useful.h>

namespace DB
{

const std::string CLUSTER_MANAGER_PATH_KEY = "flash.flash_cluster.cluster_manager_path";
const std::string BIN_NAME = "flash_cluster_manager";
const std::string TASK_INTERVAL_KEY = "flash.flash_cluster.update_rule_interval";

constexpr long MILLISECOND = 1000;
constexpr long INIT_DELAY = 5;

void ClusterManagerService::run(const std::string & bin_path, const std::vector<std::string> & args)
{
    auto proc = ShellCommand::executeDirect(bin_path, args);
    proc->wait();
}

ClusterManagerService::ClusterManagerService(DB::Context & context_, const std::string & config_path)
    : context(context_), timer(), log(&Logger::get("ClusterManagerService"))
{
    const auto & conf = context.getConfigRef();

    if (!conf.has(CLUSTER_MANAGER_PATH_KEY))
    {
        LOG_WARNING(log, "cluster manager will not run because of no config option");
        return;
    }

    auto bin_path = conf.getString(CLUSTER_MANAGER_PATH_KEY) + Poco::Path::separator() + BIN_NAME;
    auto task_interval = conf.getInt(TASK_INTERVAL_KEY, 10);

    if (!Poco::File(bin_path).exists())
    {
        LOG_ERROR(log, "binary file of cluster manager does not exist: " << bin_path);
        throw Exception("Binary file of cluster manager does not exist", ErrorCodes::LOGICAL_ERROR);
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
