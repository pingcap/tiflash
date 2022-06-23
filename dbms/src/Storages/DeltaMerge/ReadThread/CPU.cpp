#include <Storages/DeltaMerge/ReadThread/CPU.h>
#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>
#include <string>

namespace DB::DM
{
// In Linux a numa node is represented by a device directory, such as '/sys/devices/system/node/node0', ''/sys/devices/system/node/node01'.
static inline bool isNodeDir(const std::string & name)
{
    return name.substr(0, 4) == "node" && std::all_of(name.begin() + 4, name.end(), [](unsigned char c) { return std::isdigit(c); });
}

// Under a numa node directory is CPU cores and memory, such as  '/sys/devices/system/node/node0/cpu0' and '/sys/devices/system/node/node0/memory0'.
static inline bool isCPU(const std::string & name)
{
    return name.substr(0, 3) == "cpu" && std::all_of(name.begin() + 3, name.end(), [](unsigned char c) { return std::isdigit(c); });
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
std::vector<std::vector<int>> getNumaNodes()
{
    static const std::string nodes_dir_name{"/sys/devices/system/node"};
    static const std::string cpus_dir_name{"/sys/devices/system/cpu"};

    std::vector<std::vector<int>> numa_nodes;
    Poco::File nodes(nodes_dir_name);
    if (!nodes.exists() || !nodes.isDirectory())
    {
        numa_nodes.push_back(getCPUs(cpus_dir_name));
    }
    else
    {
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator iter(nodes); iter != end; ++iter)
        {
            if (isNodeDir(iter.name()))
            {
                numa_nodes.push_back(getCPUs(nodes_dir_name + "/" + iter.name()));
            }
        }
    }
    return numa_nodes;
}
}