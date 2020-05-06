#include <Common/Exception.h>
#include <Flash/DiagnosticsService.h>
#include <Flash/LogSearch.h>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <fstream>
#include <regex>
#include <thread>

#ifdef __linux__
// #include <arpa/inet.h>
// #include <ifaddrs.h>
// #include <linux/if_packet.h>
// #include <sys/socket.h>
#include <sys/statvfs.h>
#endif

namespace DB
{

using diagnosticspb::LogLevel;
using diagnosticspb::SearchLogResponse;
using diagnosticspb::ServerInfoItem;
using diagnosticspb::ServerInfoPair;
using diagnosticspb::ServerInfoResponse;
using diagnosticspb::ServerInfoType;

using MemoryInfo = std::unordered_map<std::string, uint64_t>;

namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}

namespace
{

static constexpr uint KB = 1024;
// static constexpr uint MB = 1024 * 1024;

DiagnosticsService::AvgLoad getAvgLoad()
{
    {
        Poco::File avg_load_file("/proc/loadavg");
        if (!avg_load_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/loadavg doesn't exist");
            return DiagnosticsService::AvgLoad{};
        }
    }

    std::ifstream file("/proc/loadavg");
    std::string str;
    std::getline(file, str);
    std::vector<std::string> values;
    boost::split(values, str, boost::is_any_of(" "));
    return DiagnosticsService::AvgLoad{std::stod(values[0]), std::stod(values[1]), std::stod(values[2])};
}

void getMemoryInfo(MemoryInfo & memory_info)
{
    {
        Poco::File meminfo_file("/proc/meminfo");
        if (!meminfo_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/meminfo doesn't exist");
            return;
        }
    }
    std::ifstream file("/proc/meminfo");
    std::string line;
    const std::regex pattern("([\\w\\(\\)]+):\\s+([0-9]+).*");
    std::smatch sub_matches;
    while (std::getline(file, line))
    {
        if (std::regex_match(line, sub_matches, pattern))
        {
            std::string name = sub_matches[1].str();
            uint64_t kb = std::stoul(sub_matches[2].str());
            memory_info.emplace(name, kb);
        }
    }
}

DiagnosticsService::NICInfo getNICInfo()
{
    DiagnosticsService::NICInfo nic_info;
    Poco::File net_dir("/sys/class/net");
    if (!net_dir.exists())
    {
        LOG_WARNING(&Logger::get("DiagnosticsService"), "/sys/class/net doesn't exist");
        return nic_info;
    }

    std::vector<Poco::File> devices;
    net_dir.list(devices);
    for (auto device : devices)
    {
        std::vector<Poco::File> device_infos;
        device.list(device_infos);
        auto statistics = std::find_if(device_infos.begin(), device_infos.end(), [](const Poco::File & file) {
            Poco::Path path(file.path());
            return path.getFileName() == "statistics";
        });
        std::vector<Poco::File> stat_files;
        statistics->list(stat_files);
        DiagnosticsService::NICLoad load_info;
        for (auto stat : stat_files)
        {
            Poco::Path path(stat.path());
            std::ifstream file(path.toString());
            std::string value;
            std::getline(file, value);
            if (path.getFileName() == "rx_bytes")
                load_info.rx_bytes = std::stoul(value);
            else if (path.getFileName() == "tx_bytes")
                load_info.tx_bytes = std::stoul(value);
            else if (path.getFileName() == "rx_packets")
                load_info.rx_packets = std::stoul(value);
            else if (path.getFileName() == "tx_packets")
                load_info.tx_packets = std::stoul(value);
            else if (path.getFileName() == "rx_errors")
                load_info.rx_errors = std::stoul(value);
            else if (path.getFileName() == "tx_errors")
                load_info.tx_errors = std::stoul(value);
            else if (path.getFileName() == "rx_compressed")
                load_info.rx_compressed = std::stoul(value);
            else if (path.getFileName() == "tx_compressed")
                load_info.tx_compressed = std::stoul(value);
        }

        Poco::Path device_path(device.path());
        nic_info.emplace(device_path.getFileName(), std::move(load_info));
    }
    return nic_info;
}

DiagnosticsService::IOInfo getIOInfo()
{
    DiagnosticsService::IOInfo io_info;
    Poco::File io_dir("/sys/block");
    if (!io_dir.exists())
    {
        LOG_WARNING(&Logger::get("DiagnosticsService"), "/sys/block doesn't exist");
        return io_info;
    }

    std::vector<Poco::File> devices;
    io_dir.list(devices);
    for (auto device : devices)
    {
        Poco::Path stat_file_path(device.path());
        stat_file_path.append("stat");
        Poco::File stat_file(stat_file_path);
        std::ifstream file(stat_file.path());
        std::string value;
        std::getline(file, value);
        std::vector<std::string> values;
        boost::split(values, value, boost::is_any_of("\t "), boost::token_compress_on);
        /// TODO: better to initialize with field names
        DiagnosticsService::IOLoad load{std::stod(values[0]), std::stod(values[1]), std::stod(values[2]), std::stod(values[3]),
            std::stod(values[4]), std::stod(values[5]), std::stod(values[6]), std::stod(values[7]), std::stod(values[8]),
            std::stod(values[9]), std::stod(values[10])};

        Poco::Path device_path(device.path());
        io_info.emplace(device_path.getFileName(), std::move(load));
    }
    return io_info;
}

size_t getPhysicalCoreNumber()
{
    {
        Poco::File info_file("/proc/cpuinfo");
        if (!info_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/cpuinfo doesn't exist");
            return 0;
        }
    }

    std::ifstream file("/proc/cpuinfo");
    std::string line;
    std::unordered_map<uint, size_t> map;
    uint physid = 0;
    size_t cores = 0;
    int chgcount = 0;
    while (std::getline(file, line))
    {
        std::vector<std::string> values;
        boost::split(values, line, boost::is_any_of(":"), boost::token_compress_on);
        if (values.size() >= 2)
        {
            boost::trim_left(values[0]);
            boost::trim_right(values[0]);
            boost::trim_left(values[1]);
            boost::trim_right(values[1]);
            std::string key = values[0];
            std::string value = values[1];
            if (key == "physical id")
            {
                physid = std::stoul(value);
                chgcount += 1;
            }
            else if (key == "cpu cores")
            {
                cores = std::stoul(value);
                chgcount += 1;
            }
            if (chgcount == 2)
            {
                map.insert({physid, cores});
                chgcount = 0;
            }
        }
    }

    size_t count = 0;
    for (auto [key, value] : map)
    {
        (void)key;
        count += value;
    }
    return count;
}

uint64_t getCPUFrequency()
{
    {
        Poco::File info_file("/proc/cpuinfo");
        if (!info_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/cpuinfo doesn't exist");
            return 0;
        }
    }

    std::ifstream file("/proc/cpuinfo");
    std::string line;
    while (std::getline(file, line))
    {
        std::vector<std::string> values;
        boost::split(values, line, boost::is_any_of(":\t"), boost::token_compress_on);
        if (values.size() >= 2)
        {
            std::string key = values[0];
            std::string value = values[1];
            if (key == "cpu MHz" || key == "BogoMIPS" || key == "clock" || key == "bogomips per cpu")
            {
                boost::replace_all(value, "MHz", "");
                double freq = std::stod(value);
                return static_cast<uint64_t>(freq);
            }
        }
    }
    return 0;
}

void getCacheSize(const uint & level, size_t & size, size_t & line_size)
{
    Poco::Path cache_dir("/sys/devices/system/cpu/cpu0/cache/index" + std::to_string(level));
    Poco::Path size_path = cache_dir;
    size_path.append("size");
    Poco::Path line_size_path = cache_dir;
    line_size_path.append("coherency_line_size");

    std::ifstream size_file(size_path.toString());
    std::string size_str;
    std::getline(size_file, size_str);
    // Trim the last 'K'
    size_str.resize(size_str.size() - 1);
    size = std::stoul(size_str) * KB;

    std::ifstream line_size_file(line_size_path.toString());
    std::string line_size_str;
    std::getline(line_size_file, line_size_str);
    line_size = std::stoul(line_size_str);
}

std::vector<DiagnosticsService::Disk> getAllDisks()
{
    std::vector<DiagnosticsService::Disk> disks;
#ifdef __linux__
    {
        Poco::File mount_file("/proc/mounts");
        if (!mount_file.exists())
        {
            LOG_WARNING(&Logger::get("DiagnosticsService"), "/proc/mounts doesn't exist");
            return disks;
        }
    }

    std::ifstream mount_file("/proc/mounts");
    std::string line;
    while (std::getline(mount_file, line))
    {
        boost::trim_left(line);
        if (boost::starts_with(line, "/dev/sd") || boost::starts_with(line, "/dev/nvme") || boost::starts_with(line, "/dev/mapper")
            || boost::starts_with(line, "/dev/root") || boost::starts_with(line, "/dev/mmcblk"))
        {
            std::vector<std::string> values;
            boost::split(values, line, boost::is_any_of(" "), boost::token_compress_on);
            DiagnosticsService::Disk disk;

            disk.name = values[0].substr(5, values[0].size() - 5); // trim '/dev/' prefix
            boost::trim_if(disk.name, boost::is_digit());          // trim partition number

            disk.mount_point = values[1];

            Poco::Path rotational_file_path("/sys/block/" + disk.name + "/queue/rotational");
            if (Poco::File(rotational_file_path).exists())
            {
                std::ifstream rotational_file(rotational_file_path.toString());
                std::string line;
                std::getline(rotational_file, line);
                int rotational = std::stoi(line);
                disk.disk_type = rotational == 1 ? DiagnosticsService::Disk::DiskType::HDD : DiagnosticsService::Disk::DiskType::SSD;
            }
            else
            {
                disk.disk_type = DiagnosticsService::Disk::DiskType::UNKNOWN;
            }

            disk.fs_type = values[2];

            struct statvfs stat;
            statvfs(disk.mount_point.data(), &stat);
            size_t total = static_cast<size_t>(stat.f_blocks) * static_cast<size_t>(stat.f_bsize);
            size_t available = static_cast<size_t>(stat.f_bavail) * static_cast<size_t>(stat.f_bsize);

            disk.total_space = total;
            disk.available_space = available;

            disks.emplace_back(std::move(disk));
        }
    }
#endif
    return disks;
}

// std::vector<DiagnosticsService::NetworkInterface> getNetworkInterfaces()
// {
//     std::vector<DiagnosticsService::NetworkInterface> interfaces;

//     // auto sockaddr_to_network_addr = [](const struct sockaddr * sa, std::vector<uint8_t> & mac, std::string & addr) {
//     //     if (sa->sa_family == AF_PACKET)
//     //     {
//     //         auto sll = (struct sockaddr_ll *)sa;
//     //         mac = {sll->sll_addr[0], sll->sll_addr[1], sll->sll_addr[2], sll->sll_addr[3], sll->sll_addr[4], sll->sll_addr[5]};
//     //     }
//     //     else if (sa->sa_family == AF_INET)
//     //     {
//     //         auto si = (struct sockaddr_in *)sa;
//     //         char * addr_str = inet_ntoa(si->sin_addr);
//     //         addr = std::string(addr_str);
//     //     }
//     //     else if (sa->sa_family == AF_INET6)
//     //     {
//     //         // TODO
//     //     }
//     // };

//     // struct ifaddrs * addrs;
//     // getifaddrs(&addrs);
//     // auto cur = addrs;
//     // while (cur)
//     // {
//     //     std::string name(cur->ifa_name);
//     //     std::vector<uint8_t> mac;
//     //     std::string ip;
//     //     sockaddr_to_network_addr(cur->ifa_addr, mac, ip);
//     //     std::string netmask;
//     //     std::vector<uint8_t> temp;
//     //     sockaddr_to_network_addr(cur->ifa_netmask, temp, netmask);

//     //     DiagnosticsService::NetworkInterface interface;
//     //     interface.name = name;
//     //     interface.index = 0;
//     //     interface.flag = cur->ifa_flags;
//     //     interface.mac = mac;
//     // }
//     // freeifaddrs(addrs);

//     return interfaces;
// }

} // namespace

void DiagnosticsService::cpuLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    {
        Poco::File stat_file("/proc/stat");
        if (!stat_file.exists())
        {
            // Ignore error since some os doesn't have /proc/stat
            LOG_WARNING(log, "/proc/stat doesn't exist");
            return;
        }
    }

    // Get CPU load
    {
        auto avg_load = getAvgLoad();
        std::vector<std::pair<std::string, double>> names{{"load1", avg_load.one}, {"load5", avg_load.five}, {"load15", avg_load.fifteen}};
        std::vector<ServerInfoPair> pairs;
        for (size_t i = 0; i < names.size(); i++)
        {
            ServerInfoPair pair;
            pair.set_key(names[i].first);
            pair.set_value(std::to_string(names[i].second));
            pairs.emplace_back(std::move(pair));
        }
        ServerInfoItem item;
        for (auto pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        item.set_tp("cpu");
        item.set_name("cpu");
        server_info_items.emplace_back(std::move(item));
    }

    // Get CPU stats
    {
        std::ifstream file("/proc/stat");
        std::vector<std::string> names{"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice"};

        std::string line;
        while (std::getline(file, line))
        {
            std::vector<std::string> values;
            boost::split(values, line, boost::is_any_of(" "), boost::token_compress_on);
            if (!boost::starts_with(values[0], "cpu"))
            {
                continue;
            }
            std::vector<ServerInfoPair> pairs;
            for (size_t i = 0; i < names.size(); i++)
            {
                ServerInfoPair pair;
                pair.set_key(names[i]);
                pair.set_value(values[i]);
                pairs.emplace_back(std::move(pair));
            }
            ServerInfoItem item;
            item.set_tp("cpu");
            item.set_name(values[0]);
            for (auto pair : pairs)
            {
                auto added_pair = item.add_pairs();
                added_pair->set_key(pair.key());
                added_pair->set_value(pair.value());
            }
            server_info_items.emplace_back(std::move(item));
        }
    }
}

/// TODO: wrap MemoryInfo with struct
void DiagnosticsService::memLoadInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    MemoryInfo meminfo;
    getMemoryInfo(meminfo);

    /// TODO: we should check both cgroup quota and MemTotal, then pick the less one
    uint64_t total_memory = meminfo.at("MemTotal") * KB;
    uint64_t free_memory = meminfo.at("MemFree") * KB;
    uint64_t used_memory = total_memory - free_memory;
    uint64_t total_swap = meminfo.at("SwapTotal") * KB;
    uint64_t free_swap = meminfo.at("SwapFree") * KB;
    uint64_t used_swap = total_swap - free_swap;

    char buffer[10];
    double used_memory_percent = static_cast<double>(used_memory) / static_cast<double>(total_memory);
    std::sprintf(buffer, "%.2lf", used_memory_percent);
    std::string used_memory_percent_str(buffer);
    double free_memory_percent = static_cast<double>(free_memory) / static_cast<double>(total_memory);
    std::sprintf(buffer, "%.2lf", free_memory_percent);
    std::string free_memory_percent_str(buffer);
    double used_swap_percent = static_cast<double>(used_swap) / static_cast<double>(total_swap);
    std::sprintf(buffer, "%.2lf", used_swap_percent);
    std::string used_swap_percent_str(buffer);
    double free_swap_percent = static_cast<double>(free_swap) / static_cast<double>(total_swap);
    std::sprintf(buffer, "%.2lf", free_swap_percent);
    std::string free_swap_percent_str(buffer);

    std::vector<std::pair<std::string, std::string>> memory_infos{//
        {"total", std::to_string(total_memory)},                  //
        {"free", std::to_string(free_memory)},                    //
        {"used", std::to_string(used_memory)},                    //
        {"free-percent", free_memory_percent_str},                //
        {"used-percent", used_memory_percent_str}};

    std::vector<std::pair<std::string, std::string>> swap_infos{//
        {"total", std::to_string(total_swap)},                  //
        {"free", std::to_string(free_swap)},                    //
        {"used", std::to_string(used_swap)},                    //
        {"free-percent", free_swap_percent_str},                //
        {"used-percent", used_swap_percent_str}};

    // Add pairs for memory_infos
    {
        std::vector<ServerInfoPair> pairs;
        for (auto info : memory_infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            pair.set_value(info.second);
            pairs.emplace_back(std::move(pair));
        }

        ServerInfoItem item;
        item.set_name("virtual");
        item.set_tp("memory");
        for (size_t i = 0; i < pairs.size(); i++)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pairs[i].key());
            added_pair->set_value(pairs[i].value());
        }
        server_info_items.emplace_back(std::move(item));
    }

    // Add pairs for swap_infos
    {
        std::vector<ServerInfoPair> pairs;
        for (auto info : swap_infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            pair.set_value(info.second);
            pairs.emplace_back(std::move(pair));
        }

        ServerInfoItem item;
        item.set_name("swap");
        item.set_tp("memory");
        for (size_t i = 0; i < pairs.size(); i++)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pairs[i].key());
            added_pair->set_value(pairs[i].value());
        }
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::nicLoadInfo(const NICInfo & prev_nic, std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    auto rate = [](size_t prev, size_t cur) { return static_cast<double>(cur - prev) / 0.5; };
    NICInfo nic_info = getNICInfo();
    for (auto [name, cur] : nic_info)
    {
        auto prev = prev_nic.find(name);
        if (prev == prev_nic.end())
            continue;
        std::vector<std::pair<std::string, double>> infos{
            {"rx-bytes/s", rate(cur.rx_bytes, prev->second.rx_bytes)},
            {"tx-bytes/s", rate(cur.tx_bytes, prev->second.tx_bytes)},
            {"rx-packets/s", rate(cur.rx_packets, prev->second.rx_packets)},
            {"tx-packets/s", rate(cur.tx_packets, prev->second.tx_packets)},
            {"rx-errors/s", rate(cur.rx_errors, prev->second.rx_errors)},
            {"tx-errors/s", rate(cur.tx_errors, prev->second.tx_errors)},
            {"rx-comp/s", rate(cur.rx_compressed, prev->second.rx_compressed)},
            {"tx-comp/s", rate(cur.tx_compressed, prev->second.tx_compressed)},
        };
        std::vector<ServerInfoPair> pairs;
        for (auto info : infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            char buffer[10];
            std::sprintf(buffer, "%.2lf", info.second);
            pair.set_value(std::string(buffer));
            pairs.emplace_back(std::move(pair));
        }
        ServerInfoItem item;
        for (auto pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        item.set_tp("net");
        item.set_name(name);
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::ioLoadInfo(
    const DiagnosticsService::IOInfo & prev_io, std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    auto rate = [](size_t prev, size_t cur) { return static_cast<double>(cur - prev) / 0.5; };
    IOInfo io_info = getIOInfo();
    for (auto [name, cur] : io_info)
    {
        auto prev = prev_io.find(name);
        if (prev == prev_io.end())
            continue;
        std::vector<std::pair<std::string, double>> infos{
            {"read_io/s", rate(cur.read_io, prev->second.read_io)},
            {"read_merges/s", rate(cur.read_merges, prev->second.read_merges)},
            {"read_sectors/s", rate(cur.read_sectors, prev->second.read_sectors) * 512.0},
            {"read_ticks/s", rate(cur.read_ticks, prev->second.read_ticks)},
            {"write_io/s", rate(cur.write_io, prev->second.write_ticks)},
            {"write_merges/s", rate(cur.write_merges, prev->second.write_merges)},
            {"write_sectors/s", rate(cur.write_sectors, prev->second.write_sectors) * 512.0},
            {"write_ticks/s", rate(cur.write_ticks, prev->second.write_ticks)},
            {"in_flght/s", rate(cur.in_flight, prev->second.in_flight)},
            {"io_ticks/s", rate(cur.io_ticks, prev->second.io_ticks)},
            {"time_in_queue/s", rate(cur.time_in_queue, prev->second.time_in_queue)},
        };
        std::vector<ServerInfoPair> pairs;
        for (auto info : infos)
        {
            ServerInfoPair pair;
            pair.set_key(info.first);
            char buffer[10];
            std::sprintf(buffer, "%.2lf", info.second);
            pair.set_value(std::string(buffer));
            pairs.emplace_back(std::move(pair));
        }
        ServerInfoItem item;
        for (auto pair : pairs)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
        item.set_tp("io");
        item.set_name(name);
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::loadInfo(
    const NICInfo & prev_nic, const IOInfo & prev_io, std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    cpuLoadInfo(server_info_items);
    memLoadInfo(server_info_items);
    (void)prev_nic;
    (void)prev_io;
    // nicLoadInfo(prev_nic, server_info_items);
    // ioLoadInfo(prev_io, server_info_items);
}

void DiagnosticsService::cpuHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    std::vector<std::pair<std::string, std::string>> infos{//
        {"cpu-logical-cores",
            std::to_string(std::thread::hardware_concurrency())}, /// TODO: we should check both machine's core number and cgroup quota
        {"cpu-physical-cores", std::to_string(getPhysicalCoreNumber())}, //
        {"cpu-frequency", std::to_string(getCPUFrequency()) + "MHz"}};

    // L1 to L3 cache
    for (uint8_t level = 1; level <= 3; level++)
    {
        size_t cache_size;
        size_t cache_line_size;
        getCacheSize(level, cache_size, cache_line_size);
        std::string level_str = "l" + std::to_string(level);
        infos.emplace_back(level_str + "-cache-size", std::to_string(cache_size));
        infos.emplace_back(level_str + "-cache-line-size", std::to_string(cache_line_size));
    }

    ServerInfoItem item;
    for (auto info : infos)
    {
        auto pair = item.add_pairs();
        pair->set_key(info.first);
        pair->set_value(info.second);
    }
    item.set_name("cpu");
    item.set_tp("cpu");
    server_info_items.emplace_back(std::move(item));
}

void DiagnosticsService::memHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    MemoryInfo mem_info;
    getMemoryInfo(mem_info);
    size_t total_mem = mem_info.at("MemTotal");

    ServerInfoItem item;
    auto pair = item.add_pairs();
    pair->set_key("capacity");
    pair->set_value(std::to_string(total_mem * KB));
    item.set_name("memory");
    item.set_tp("memory");

    server_info_items.emplace_back(std::move(item));
}

void DiagnosticsService::diskHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    std::vector<Disk> all_disks = getAllDisks();

    std::string data_path = server.config().getString("path");
    std::vector<std::string> data_dirs;
    boost::split(data_dirs, data_path, boost::is_any_of(","));

    std::vector<Disk> disks_in_use;
    disks_in_use.reserve(all_disks.size());

    for (auto disk : all_disks)
    {
        bool is_in_use = false;
        for (auto dir : data_dirs)
        {
            if (boost::starts_with(dir, disk.mount_point))
            {
                is_in_use = true;
                break;
            }
        }

        if (is_in_use)
            disks_in_use.emplace_back(std::move(disk));
    }

    for (auto disk : disks_in_use)
    {
        size_t total = disk.total_space;
        size_t free = disk.available_space;
        size_t used = total - free;

        char buffer[10];
        double free_percent = static_cast<double>(free) / static_cast<double>(total);
        std::sprintf(buffer, "%.2lf", free_percent);
        std::string free_percent_str(buffer);
        double used_percent = static_cast<double>(used) / static_cast<double>(total);
        std::sprintf(buffer, "%.2lf", used_percent);
        std::string used_percent_str(buffer);

        std::vector<std::pair<std::string, std::string>> infos{{"type", disk.disk_type == disk.HDD ? "HDD" : "SSD"},
            {"fstype", disk.fs_type}, {"path", disk.mount_point}, {"total", std::to_string(total)}, {"free", std::to_string(free)},
            {"used", std::to_string(used)}, {"free-percent", free_percent_str}, {"used-percent", used_percent_str}};

        ServerInfoItem item;
        for (auto info : infos)
        {
            auto added_pair = item.add_pairs();
            added_pair->set_key(info.first);
            added_pair->set_value(info.second);
        }
        item.set_tp("disk");
        item.set_name(disk.name);
        server_info_items.emplace_back(std::move(item));
    }
}

void DiagnosticsService::nicHardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items) { (void)server_info_items; }

void DiagnosticsService::hardwareInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items)
{
    cpuHardwareInfo(server_info_items);
    memHardwareInfo(server_info_items);
    diskHardwareInfo(server_info_items);
}

void DiagnosticsService::systemInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items) { (void)server_info_items; }

void DiagnosticsService::processInfo(std::vector<diagnosticspb::ServerInfoItem> & server_info_items) { (void)server_info_items; }

::grpc::Status DiagnosticsService::server_info(
    ::grpc::ServerContext * context, const ::diagnosticspb::ServerInfoRequest * request, ::diagnosticspb::ServerInfoResponse * response) try
{
    (void)context;

    auto tp = request->tp();
    std::vector<ServerInfoItem> items;

    IOInfo io_info;
    NICInfo nic_info;

    if (tp == ServerInfoType::LoadInfo || tp == ServerInfoType::All)
    {
        // io_info = getIOInfo();
        // nic_info = getNICInfo();
        // Sleep 100ms to sample
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    switch (tp)
    {
        case ServerInfoType::LoadInfo:
        {
            loadInfo(nic_info, io_info, items);
            break;
        }
        case ServerInfoType::HardwareInfo:
        {
            hardwareInfo(items);
            break;
        }
        case ServerInfoType::SystemInfo:
        {
            systemInfo(items);
            break;
        }
        case ServerInfoType::All:
        {
            loadInfo(nic_info, io_info, items);
            hardwareInfo(items);
            systemInfo(items);
            break;
        }
        default:
            break;
    }

    std::sort(items.begin(), items.end(), [](const ServerInfoItem & lhs, const ServerInfoItem & rhs) -> bool {
        auto l = std::make_pair(lhs.tp(), lhs.name());
        auto r = std::make_pair(rhs.tp(), rhs.name());
        return l < r;
    });

    // auto resp = ServerInfoResponse::default_instance();
    auto & resp = *response;
    for (auto item : items)
    {
        auto added_item = resp.add_items();
        added_item->set_name(item.name());
        added_item->set_tp(item.tp());
        for (auto pair : item.pairs())
        {
            auto added_pair = added_item->add_pairs();
            added_pair->set_key(pair.key());
            added_pair->set_value(pair.value());
        }
    }

    return ::grpc::Status::OK;
}
catch (const Exception & e)
{
    LOG_ERROR(log, e.displayText());
    return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
}

::grpc::Status DiagnosticsService::search_log(::grpc::ServerContext * grpc_context, const ::diagnosticspb::SearchLogRequest * request,
    ::grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * stream)
{
    (void)grpc_context;

    /// TODO: add error log
    Poco::File log_file(Poco::Path(server.config().getString("logger.log")));
    if (!log_file.exists())
    {
        LOG_ERROR(log, "Invalid log path: " << log_file.path());
        return ::grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
    }

    int64_t start_time = request->start_time();
    int64_t end_time = request->end_time();
    if (end_time == 0)
    {
        // default to now
        end_time = std::chrono::milliseconds(std::time(NULL)).count();
    }
    std::vector<LogLevel> levels;
    for (auto level : request->levels())
    {
        levels.push_back(static_cast<LogLevel>(level));
    }

    std::vector<std::string> patterns;
    for (auto pattern : request->patterns())
    {
        patterns.push_back(pattern);
    }

    auto in_ptr = std::shared_ptr<std::ifstream>(new std::ifstream(log_file.path()));

    LogIterator log_itr(start_time, end_time, levels, patterns, in_ptr);

    static constexpr size_t LOG_BATCH_SIZE = 256;

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling SearchLog: " << request->DebugString());
    for (;;)
    {
        size_t i = 0;
        auto resp = SearchLogResponse::default_instance();
        while (auto log_msg = log_itr.next())
        {
            i++;
            auto added_msg = resp.add_messages();
            *added_msg = *log_msg;

            if (i == LOG_BATCH_SIZE - 1)
                break;
        }

        if (i == 0)
            break;

        if (!stream->Write(resp))
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Write response failed for unknown reason.");
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write response failed for unknown reason.");
        }
    }
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling SearchLog done: " << request->DebugString());

    return ::grpc::Status::OK;
}

} // namespace DB
