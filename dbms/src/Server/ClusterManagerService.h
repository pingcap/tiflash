#pragma once

#include <Common/Timer.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <common/logger_useful.h>

namespace DB
{

class Context;
class BackgroundProcessingPool;

class ClusterManagerService : private boost::noncopyable
{
public:
    ClusterManagerService(Context & context_, const std::string & config_path);
    ~ClusterManagerService();

private:
    static void run(const std::string & command, const std::vector<std::string> & args);
    Context & context;
    Timer timer;
    Logger * log;
};


} // namespace DB
