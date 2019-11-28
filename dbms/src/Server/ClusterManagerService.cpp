#include "ClusterManagerService.h"


#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>
#include <Common/FunctionTimerTask.h>
#include <Common/ShellCommand.h>
#include <Poco/Path.h>
#include <Poco/File.h>
#include <common/logger_useful.h>

namespace DB
{

const std::string CLUSTER_MANAGER_PATH_KEY = "flash.flash_cluster.cluster_manager_path";
const std::string BIN_NAME = "flash_cluster_manager";
const std::string TASK_INTERVAL_KEY = "flash.flash_cluster.update_rule_interval";

constexpr long SECOND = 1000;
constexpr long INIT_DELAY = SECOND * 5;

void ClusterManagerService::run(const std::string & bin_path, const std::vector<std::string> & args)
{
    auto proc = ShellCommand::executeDirect(bin_path, args);
    proc->wait();
}

ClusterManagerService::ClusterManagerService(DB::Context & context_, const std::string config_path)
    : context(context_), timer(), log(&Logger::get("ClusterManagerService"))
{
    const auto & conf = context.getConfigRef();

    auto bin_path = conf.getString(CLUSTER_MANAGER_PATH_KEY, ".") + Poco::Path::separator() + BIN_NAME;
    auto task_interval = conf.getInt(TASK_INTERVAL_KEY, 10) * SECOND;

    if (!Poco::File(bin_path).exists())
        throw Exception("Cluster manager binary file does not exist: " + bin_path);

    std::vector<std::string> args;
    args.push_back(config_path);

    LOG_INFO(log, "Registered timed cluster manager task at rate " << task_interval / SECOND << " seconds");

    timer.scheduleAtFixedRate(FunctionTimerTask::create(std::bind(&ClusterManagerService::run, bin_path, args)), INIT_DELAY, task_interval);
}

ClusterManagerService::~ClusterManagerService() { timer.cancel(true); }

} // namespace DB
