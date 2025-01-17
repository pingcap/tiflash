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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/ReadThread/CPU.h>
#include <common/logger_useful.h>
#include <stdlib.h>

#include <exception>
#include <string>

namespace DB::DM
{
// In Linux a numa node is represented by a device directory, such as '/sys/devices/system/node/node0', '/sys/devices/system/node/node01'.
static inline bool isNodeDir(const std::string & name)
{
    return name.size() > 4 && name.substr(0, 4) == "node"
        && std::all_of(name.begin() + 4, name.end(), [](unsigned char c) { return std::isdigit(c); });
}

// Under a numa node directory is CPU cores and memory, such as  '/sys/devices/system/node/node0/cpu0' and '/sys/devices/system/node/node0/memory0'.
static inline bool isCPU(const std::string & name)
{
    return name.size() > 3 && name.substr(0, 3) == "cpu"
        && std::all_of(name.begin() + 3, name.end(), [](unsigned char c) { return std::isdigit(c); });
}

static inline int parseCPUNumber(const std::string & name)
{
    return std::stoi(name.substr(3));
}

// Scan the numa node directory and parse the CPU numbers.
static inline std::vector<int> getCPUs(const std::string & dir_name)
{
    std::vector<int> cpus;
    Poco::File dir(dir_name);
    if (!dir.exists())
        return cpus;
    Poco::DirectoryIterator end;
    for (auto iter = Poco::DirectoryIterator(dir); iter != end; ++iter)
    {
        if (isCPU(iter.name()))
        {
            cpus.push_back(parseCPUNumber(iter.name()));
        }
    }
    return cpus;
}

// TODO: What if the process running in the container and the CPU is limited.

// Scan the device directory and parse the CPU information.
std::vector<std::vector<int>> getLinuxNumaNodes()
{
    static const std::string nodes_dir_name{"/sys/devices/system/node"};
    static const std::string cpus_dir_name{"/sys/devices/system/cpu"};

    std::vector<std::vector<int>> numa_nodes;
    Poco::File nodes(nodes_dir_name);
    if (!nodes.exists() || !nodes.isDirectory())
    {
        auto cpus = getCPUs(cpus_dir_name);
        RUNTIME_CHECK_MSG(!cpus.empty(), "Not recognize CPU: {}", cpus_dir_name);
        numa_nodes.push_back(std::move(cpus));
        return numa_nodes;
    }

    // get the cpu id from each NUMA node
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator iter(nodes); iter != end; ++iter)
    {
        if (!isNodeDir(iter.name()))
        {
            continue;
        }
        auto dir_name = nodes_dir_name + "/" + iter.name();
        auto cpus = getCPUs(dir_name);
        RUNTIME_CHECK_MSG(!cpus.empty(), "Not recognize CPU: {}", nodes_dir_name);
        numa_nodes.push_back(std::move(cpus));
    }
    RUNTIME_CHECK_MSG(!numa_nodes.empty(), "Not recognize CPU");
    return numa_nodes;
}

std::vector<std::vector<int>> getNumaNodes(const LoggerPtr & log)
{
#ifndef __APPLE__ // Apple macbooks does not support NUMA
    try
    {
        return getLinuxNumaNodes();
    }
    catch (Exception & e)
    {
        LOG_WARNING(log, "{}", e.message());
    }
    catch (std::exception & e)
    {
        LOG_WARNING(log, "{}", e.what());
    }
    catch (...)
    {
        LOG_WARNING(log, "Unknown Error");
    }
#endif
    LOG_WARNING(log, "Cannot recognize the CPU NUMA infomation, use the CPU as 'one numa node'");
    std::vector<std::vector<int>> numa_nodes(1); // "One numa node"
    return numa_nodes;
}

void setCPUAffinity(const std::vector<int> & cpus, const LoggerPtr & log)
{
    if (cpus.empty())
    {
        return;
    }
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    for (int i : cpus)
    {
        CPU_SET(i, &cpu_set);
    }
    int ret = sched_setaffinity(0, sizeof(cpu_set), &cpu_set);
    if (ret != 0)
    {
        // It can be failed due to some CPU core cannot access, such as CPU offline.
        LOG_WARNING(log, "sched_setaffinity fail, cpus={} errno={}", cpus, errno);
    }
    else
    {
        LOG_DEBUG(log, "sched_setaffinity succ, cpus={}", cpus);
    }
#endif
}
} // namespace DB::DM
